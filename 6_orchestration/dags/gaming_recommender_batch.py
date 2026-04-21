"""
Airflow DAG - Gaming Recommender Batch Pipeline (Hybrid CF + CB + Agentes)
-----------------------------------------------------------------------------
Orquesta diariamente el recomendador hibrido a las 02:30 UTC (30 min despues
del DAG original gaming_kdd_batch_pipeline, asi no se pisan recursos):

  1. Validar que existan los datasets de Kaggle (descarga/sintetico si hace falta).
  2. Validar topics de Kafka del recomendador.
  3. Validar keyspace gaming_recommender en Cassandra (crea si falta).
  4. Cargar CSVs Kaggle -> HDFS/Hive (loader).
  5. Ejecutar pipeline KDD completo: ALS + TF-IDF + Hybrid + Evaluator.
  6. Validar que hay recomendaciones en Cassandra.
  7. Llamar al agente Monitor (drift / diagnostico IA).
"""

from datetime import datetime, timedelta, timezone

from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

PROJECT_DIR = "/home/hadoop/juegosKDD"
VENV = f"{PROJECT_DIR}/venv"
PYTHON = f"{VENV}/bin/python"
SPARK = "/opt/spark/bin/spark-submit"
CQLSH = "cqlsh"
KAFKA_TOPIC = "/opt/kafka/bin/kafka-topics.sh"

ENV_PREFIX = (
    f"export PATH={VENV}/bin:/opt/spark/bin:/opt/kafka/bin:$PATH && "
    f"source {VENV}/bin/activate && "
)

PACKAGES = "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"

default_args = {
    "owner": "kdd_recommender",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=3),
}


with DAG(
    dag_id="gaming_recommender_batch_pipeline",
    default_args=default_args,
    description="Hybrid ALS + TF-IDF recommender (KDD 5 fases)",
    schedule="30 2 * * *",
    start_date=datetime(2026, 4, 20, tzinfo=timezone.utc),
    catchup=False,
    tags=["kdd", "recommender", "hybrid", "lambda"],
    doc_md=__doc__,
) as dag:

    # 1. Datasets Kaggle (descarga o sintetico) + validacion
    download_datasets = BashOperator(
        task_id="download_datasets",
        bash_command=(
            f"{ENV_PREFIX}"
            f"cd {PROJECT_DIR} && "
            f"{PYTHON} 0_data/kaggle/download_datasets.py && "
            f"{PYTHON} 0_data/kaggle/validate_datasets.py"
        ),
    )

    # 2. Topics Kafka del recomendador
    check_kafka_topics = BashOperator(
        task_id="check_kafka_topics",
        bash_command=(
            f"{ENV_PREFIX}"
            f"{KAFKA_TOPIC} --bootstrap-server localhost:9092 --list "
            "| grep -q gaming.user.interactions "
            "|| (bash "
            f"{PROJECT_DIR}/2_kafka/setup_topics.sh)"
        ),
    )

    # 3. Cassandra keyspace
    check_cassandra = BashOperator(
        task_id="check_cassandra_keyspace",
        bash_command=(
            f"{ENV_PREFIX}"
            f"{CQLSH} -e 'DESCRIBE KEYSPACES;' | grep -q gaming_recommender "
            f"|| {CQLSH} -f {PROJECT_DIR}/5_serving_layer/recommender_schema.cql"
        ),
    )

    # 4. Loader CSV -> HDFS/Hive
    load_hive = BashOperator(
        task_id="load_kaggle_to_hive",
        bash_command=(
            f"{ENV_PREFIX}"
            f"bash {PROJECT_DIR}/1_ingesta/kaggle_to_hive/run_loader.sh"
        ),
    )

    # 5. Pipeline KDD (ALS + TF-IDF + Hybrid + metricas)
    run_kdd = BashOperator(
        task_id="run_kdd_hybrid_pipeline",
        bash_command=(
            f"{ENV_PREFIX}"
            f"cd {PROJECT_DIR} && "
            f"{SPARK} "
            "--master local[*] "
            "--driver-memory 4g --executor-memory 4g "
            f"--packages {PACKAGES} "
            "--conf spark.cassandra.connection.host=localhost "
            "--py-files "
            f"{PROJECT_DIR}/4_batch_layer/recommender/collaborative_filtering.py,"
            f"{PROJECT_DIR}/4_batch_layer/recommender/content_based.py,"
            f"{PROJECT_DIR}/4_batch_layer/recommender/hybrid_recommender.py,"
            f"{PROJECT_DIR}/4_batch_layer/recommender/model_evaluator.py "
            f"{PROJECT_DIR}/4_batch_layer/recommender/kdd_pipeline.py "
            "--date={{ ds }} --alpha 0.7"
        ),
    )

    # 6. Validacion de resultados en Cassandra
    validate_cassandra = BashOperator(
        task_id="validate_cassandra_output",
        bash_command=r"""
COUNT=$(cqlsh -e "SELECT COUNT(*) FROM gaming_recommender.user_recommendations;" \
        | grep -oP '[0-9]+' | tail -1)
echo "user_recommendations: $COUNT filas"
if [ -z "$COUNT" ] || [ "$COUNT" -lt "1" ]; then
    echo "ERROR: ninguna recomendacion en Cassandra"
    exit 1
fi
""",
    )

    # 7. Agente Monitor + (opcional) trigger automatico
    run_monitor = BashOperator(
        task_id="run_monitor_agent",
        bash_command=(
            "curl -s 'http://localhost:8000/api/agent/monitor' | head -c 1500 "
            "|| echo 'FastAPI no disponible, monitor skipped'"
        ),
    )

    (
        download_datasets
        >> [check_kafka_topics, check_cassandra]
        >> load_hive
        >> run_kdd
        >> validate_cassandra
        >> run_monitor
    )
