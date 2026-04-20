"""
Airflow DAG — Gaming KDD Batch Pipeline
-----------------------------------------------------------------------------
Orquesta el Batch Layer diariamente a las 02:00 UTC:

  1. Verifica que Kafka, Cassandra y Hive estén disponibles.
  2. Ejecuta un batch adicional de Steam → Kafka (complementa NiFi).
  3. Drena Kafka → HDFS/Hive (player_snapshots) — sustituye a NiFi/Kafka Connect.
  4. Repara particiones en Hive.
  5. Lanza el Spark Batch KDD (5 fases).
  6. Valida que los resultados llegaron a Cassandra.
  7. Genera un reporte diario en los logs del DAG.
"""

from datetime import datetime, timedelta, timezone

from airflow import DAG

try:
    # Airflow 3.x (paquete standard separado)
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    # Airflow 2.x fallback
    from airflow.operators.bash import BashOperator

# ── Paths reales de la instalación Linux Mint del usuario ───────────────────
PROJECT_DIR = "/home/hadoop/juegosKDD"
VENV        = f"{PROJECT_DIR}/venv"
PYTHON      = f"{VENV}/bin/python"
SPARK       = "/opt/spark/bin/spark-submit"
SPARK_SQL   = "/opt/spark/bin/spark-sql"
HIVE        = "/home/hadoop/apache-hive-4.2.0-bin/bin/hive"
KAFKA_TOPIC = "/opt/kafka/bin/kafka-topics.sh"
CQLSH       = "cqlsh"  # disponible en PATH (Entorno Smart Grid)

# Prefijo común que garantiza PATH y activación del venv en cada BashOperator.
ENV_PREFIX = (
    f"export PATH={VENV}/bin:/opt/spark/bin:/opt/kafka/bin:"
    f"/home/hadoop/apache-hive-4.2.0-bin/bin:$PATH && "
    f"source {VENV}/bin/activate && "
)

default_args = {
    "owner":             "kdd_gaming",
    "depends_on_past":   False,
    "email_on_failure":  False,
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="gaming_kdd_batch_pipeline",
    default_args=default_args,
    description="KDD Batch Layer — Gaming Analytics (Steam + SteamSpy)",
    schedule="0 2 * * *",
    start_date=datetime(2026, 4, 19, tzinfo=timezone.utc),
    catchup=False,
    tags=["kdd", "gaming", "lambda", "batch"],
    doc_md=__doc__,
) as dag:

    # ── 1. Verificar disponibilidad de servicios ─────────────────────────────
    check_kafka = BashOperator(
        task_id="check_kafka",
        bash_command=(
            f"{ENV_PREFIX}"
            f"{KAFKA_TOPIC} --bootstrap-server localhost:9092 --list "
            "| grep gaming.events.raw"
        ),
    )

    check_cassandra = BashOperator(
        task_id="check_cassandra",
        bash_command=(
            f"{ENV_PREFIX}"
            f"{CQLSH} -e 'DESCRIBE KEYSPACES;' | grep gaming_kdd"
        ),
    )

    # Metastore Thrift en :9083 (el Batch Layer no necesita HS2)
    check_hive_metastore = BashOperator(
        task_id="check_hive_metastore",
        bash_command=(
            f"{ENV_PREFIX}"
            "ss -ltn | grep -q ':9083' "
            "&& echo 'Metastore OK' "
            "|| (echo 'Metastore no responde en :9083' && exit 1)"
        ),
    )

    # ── 2. Ingesta batch adicional desde Steam → Kafka ───────────────────────
    ingest_steam = BashOperator(
        task_id="ingest_steam_batch",
        bash_command=(
            f"{ENV_PREFIX}"
            f"cd {PROJECT_DIR}/1_ingesta/producer && "
            f"{PYTHON} steam_producer.py --mode=batch"
        ),
    )

    # ── 3. Drenar Kafka → HDFS/Hive (player_snapshots) ───────────────────────
    kafka_to_hive = BashOperator(
        task_id="kafka_to_hive",
        bash_command=(
            f"{ENV_PREFIX}"
            f"bash {PROJECT_DIR}/4_batch_layer/submit_kafka_to_hive.sh "
            "{{ ds }}"
        ),
    )

    # ── 4. Reparar particiones en Hive (via spark-sql: más rápido que HS2) ──
    hive_repair = BashOperator(
        task_id="hive_repair_partitions",
        bash_command=(
            f"{ENV_PREFIX}"
            f"{SPARK_SQL} -e \""
            "MSCK REPAIR TABLE gaming_kdd.player_snapshots;\""
        ),
    )

    # ── 5. Spark Batch KDD ───────────────────────────────────────────────────
    spark_kdd_batch = BashOperator(
        task_id="spark_kdd_batch",
        bash_command=(
            f"{ENV_PREFIX}"
            f"{SPARK} "
            "--master local[*] "
            "--driver-memory 2g --executor-memory 2g "
            "--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 "
            "--conf spark.cassandra.connection.host=localhost "
            f"{PROJECT_DIR}/4_batch_layer/spark_batch_kdd.py "
            "--date={{ ds }}"
        ),
    )

    # ── 6. Validar resultados en Cassandra ───────────────────────────────────
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

    # ── 7. Generar reporte del día ───────────────────────────────────────────
    daily_report = BashOperator(
        task_id="generate_daily_report",
        bash_command=(
            f"{ENV_PREFIX}"
            f"cd {PROJECT_DIR}/6_orchestration/dags && "
            f"DT={{{{ ds }}}} {PYTHON} _daily_report.py"
        ),
    )

    # ── DAG dependencies ─────────────────────────────────────────────────────
    [check_kafka, check_cassandra, check_hive_metastore] >> ingest_steam
    ingest_steam >> kafka_to_hive >> hive_repair >> spark_kdd_batch
    spark_kdd_batch >> validate_results >> daily_report
