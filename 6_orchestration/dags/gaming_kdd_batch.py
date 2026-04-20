"""
Airflow DAG — Gaming KDD Batch Pipeline
-----------------------------------------------------------------------------
Orquesta el Batch Layer diariamente a las 02:00 UTC:

  1. Verifica que Kafka, Cassandra y Hive estén disponibles.
  2. Ejecuta un batch adicional de Steam → Kafka (complementa NiFi).
  3. Añade la partición del día a Hive.
  4. Lanza el Spark Batch KDD (5 fases).
  5. Valida que los resultados llegaron a Cassandra.
  6. Genera un reporte diario en los logs del DAG.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

PROJECT_DIR = "/opt/kdd-lambda-gaming"
PYTHON      = "/usr/bin/python3"
SPARK       = "/opt/spark/bin/spark-submit"
HIVE        = "/opt/hive/bin/hive"
KAFKA       = "/opt/kafka/bin/kafka-topics.sh"

default_args = {
    "owner":             "kdd_gaming",
    "depends_on_past":   False,
    "email_on_failure":  False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="gaming_kdd_batch_pipeline",
    default_args=default_args,
    description="KDD Batch Layer — Gaming Analytics (Steam + SteamSpy)",
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["kdd", "gaming", "lambda", "batch"],
    doc_md=__doc__,
) as dag:

    # ── Verificar disponibilidad de servicios ─────────────────────────────────
    check_kafka = BashOperator(
        task_id="check_kafka",
        bash_command=(
            f"{KAFKA} --bootstrap-server localhost:9092 --list "
            "| grep gaming.events.raw"
        ),
    )

    check_cassandra = BashOperator(
        task_id="check_cassandra",
        bash_command="cqlsh -e 'DESCRIBE KEYSPACES;' | grep gaming_kdd",
    )

    check_hive = BashOperator(
        task_id="check_hive",
        bash_command=(
            f"{HIVE} -e 'USE gaming_kdd; SHOW TABLES;' "
            "| grep player_snapshots"
        ),
    )

    # ── Ingesta batch adicional desde Steam → Kafka ──────────────────────────
    ingest_steam = BashOperator(
        task_id="ingest_steam_batch",
        bash_command=(
            f"cd {PROJECT_DIR}/1_ingesta/producer && "
            f"{PYTHON} steam_producer.py --mode=batch --date={{{{ ds }}}}"
        ),
    )

    # ── Reparar partición en Hive ─────────────────────────────────────────────
    hive_add_partition = BashOperator(
        task_id="hive_add_partition",
        bash_command=(
            f"{HIVE} -e \""
            "USE gaming_kdd; "
            "ALTER TABLE player_snapshots "
            "ADD IF NOT EXISTS PARTITION (dt='{{ ds }}') "
            "LOCATION '/user/hive/warehouse/gaming_kdd/player_snapshots/dt={{ ds }}'; "
            "MSCK REPAIR TABLE player_snapshots;"
            "\""
        ),
    )

    # ── Spark Batch KDD ───────────────────────────────────────────────────────
    spark_kdd_batch = BashOperator(
        task_id="spark_kdd_batch",
        bash_command=(
            f"{SPARK} "
            "--master local[*] "
            "--driver-memory 2g --executor-memory 2g "
            "--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 "
            "--conf spark.cassandra.connection.host=localhost "
            f"{PROJECT_DIR}/4_batch_layer/spark_batch_kdd.py "
            "--date={{ ds }}"
        ),
    )

    # ── Validar resultados en Cassandra ───────────────────────────────────────
    validate_results = BashOperator(
        task_id="validate_cassandra_results",
        bash_command=r"""
COUNT=$(cqlsh -e "SELECT COUNT(*) FROM gaming_kdd.game_stats_daily WHERE dt='{{ ds }}' ALLOW FILTERING;" \
        | grep -oP '\d+' | tail -1)
echo "Registros en Cassandra para {{ ds }}: $COUNT"
if [ -z "$COUNT" ] || [ "$COUNT" -lt "1" ]; then
    echo "ERROR: No se encontraron registros en Cassandra"
    exit 1
fi
""",
    )

    # ── Generar reporte del día ───────────────────────────────────────────────
    # El script lee game_stats_daily y lo imprime formateado en los logs.
    daily_report = BashOperator(
        task_id="generate_daily_report",
        bash_command=(
            f"cd {PROJECT_DIR}/6_orchestration/dags && "
            f"DT={{{{ ds }}}} {PYTHON} _daily_report.py"
        ),
    )

    [check_kafka, check_cassandra, check_hive] >> ingest_steam
    ingest_steam >> hive_add_partition >> spark_kdd_batch
    spark_kdd_batch >> validate_results >> daily_report
