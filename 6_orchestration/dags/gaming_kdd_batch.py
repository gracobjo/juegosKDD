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
# IVY2_HOME: mismo motivo que en submit_kafka_to_hive.sh — `metastore.jars=maven`
# en spark-submit del batch KDD debe escribir Ivy en un sitio fijo y con
# permisos predecibles (Airflow a veces ejecuta como root).
ENV_PREFIX = (
    f"export IVY2_HOME={PROJECT_DIR}/0_infra/.ivy2 && mkdir -p \"$IVY2_HOME\" && "
    f"export PATH={VENV}/bin:/opt/spark/bin:/opt/kafka/bin:"
    f"/home/hadoop/apache-hive-4.2.0-bin/bin:$PATH && "
    f"source {VENV}/bin/activate && "
)

# Fecha de proceso: en Airflow 3.x los manual runs sin --logical-date NO tienen
# `logical_date`, `data_interval_start` ni `data_interval_end` (todos dan
# UndefinedError al renderizar `{{ ds }}`). En cambio `dag_run.run_after` SÍ
# está siempre definido (es el timestamp de encolado en un manual, y el fin
# del intervalo en un scheduled run). Lo normalizamos a YYYY-MM-DD para
# usarlo como partición.
DS = "{{ dag_run.run_after.strftime('%Y-%m-%d') }}"

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
            f"{DS}"
        ),
    )

    # Cliente Hive forzado a 3.1.3 para hablar con el Metastore 4.2.0. El
    # cliente embebido 2.3 que trae Spark 3.5.1 revienta con "Invalid method
    # name: 'get_table'" (Hive 4 renombró a get_table_req). Spark 3.5.1 solo
    # admite metastore.version ∈ [0.12–2.3.9] ∪ [3.0.0–3.1.3], así que
    # usamos 3.1.3 (compat legacy con el Metastore 4). Los jars se bajan de
    # Maven la primera vez y quedan cacheados.
    HIVE_METASTORE_CONF = (
        "--conf spark.sql.hive.metastore.version=3.1.3 "
        "--conf spark.sql.hive.metastore.jars=maven "
    )

    # ── 4. Reparar particiones en Hive (beeline → HS2 nativo) ──────────────
    # Antes esto era `spark-sql ... -e "MSCK REPAIR TABLE ..."` pero arrancar
    # un SparkSession solo para una sentencia DDL trivial es absurdo: con la
    # config `spark.sql.hive.metastore.jars=maven` Spark se descarga ~200MB
    # de jars en cada invocación y, peor, deja el job colgado contra el
    # Metastore 4.x bloqueando recursos. `beeline` se conecta directamente a
    # HiveServer2 (Hive 4.2.0 nativo, sin mismatch de versiones), ejecuta
    # el MSCK en milisegundos y se desconecta.
    hive_repair = BashOperator(
        task_id="hive_repair_partitions",
        bash_command=(
            f"{ENV_PREFIX}"
            "beeline -u 'jdbc:hive2://localhost:10000/gaming_kdd' "
            "--silent=true --showHeader=false --outputformat=tsv2 "
            "-e 'MSCK REPAIR TABLE gaming_kdd.player_snapshots;'"
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
            f"{HIVE_METASTORE_CONF}"
            f"{PROJECT_DIR}/4_batch_layer/spark_batch_kdd.py "
            f"--date={DS}"
        ),
    )

    # ── 6. Validar resultados en Cassandra ───────────────────────────────────
    validate_results = BashOperator(
        task_id="validate_cassandra_results",
        bash_command=(
            "DT=" + DS + r""" ;
COUNT=$(cqlsh -e "SELECT COUNT(*) FROM gaming_kdd.game_stats_daily WHERE dt='$DT' ALLOW FILTERING;" \
        | grep -oP '\d+' | tail -1)
echo "Registros en Cassandra para $DT: $COUNT"
if [ -z "$COUNT" ] || [ "$COUNT" -lt "1" ]; then
    echo "ERROR: No se encontraron registros en Cassandra"
    exit 1
fi
"""
        ),
    )

    # ── 7. Generar reporte del día ───────────────────────────────────────────
    daily_report = BashOperator(
        task_id="generate_daily_report",
        bash_command=(
            f"{ENV_PREFIX}"
            f"cd {PROJECT_DIR}/6_orchestration/dags && "
            f"DT={DS} {PYTHON} _daily_report.py"
        ),
    )

    # ── DAG dependencies ─────────────────────────────────────────────────────
    [check_kafka, check_cassandra, check_hive_metastore] >> ingest_steam
    ingest_steam >> kafka_to_hive >> hive_repair >> spark_kdd_batch
    spark_kdd_batch >> validate_results >> daily_report
