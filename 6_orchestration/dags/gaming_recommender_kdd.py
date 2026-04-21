"""
DAG opcional — Recomendador híbrido (Kaggle → Hive → ALS+TF-IDF → Cassandra).
Requiere datos en gaming_recommender.* y keyspace Cassandra gaming_recommender.
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
KAFKA_TOPIC = "/opt/kafka/bin/kafka-topics.sh"
CQLSH = "cqlsh"

ENV_PREFIX = (
    f"export PATH={VENV}/bin:/opt/spark/bin:/opt/kafka/bin:$PATH && "
    f"source {VENV}/bin/activate && "
)

default_args = {
    "owner": "kdd_gaming",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=4),
}

with DAG(
    dag_id="gaming_recommender_kdd_pipeline",
    default_args=default_args,
    description="KDD híbrido — ALS + TF-IDF + Cassandra (gaming_recommender)",
    schedule="0 3 * * *",
    start_date=datetime(2026, 4, 19, tzinfo=timezone.utc),
    catchup=False,
    tags=["kdd", "recommender", "hybrid"],
    doc_md=__doc__,
) as dag:

    check_kafka = BashOperator(
        task_id="check_kafka_interactions_topic",
        bash_command=(
            f"{ENV_PREFIX}{KAFKA_TOPIC} --bootstrap-server localhost:9092 --list "
            "| grep gaming.user.interactions"
        ),
    )

    check_cassandra = BashOperator(
        task_id="check_cassandra_recommender_ks",
        bash_command=(
            f"{ENV_PREFIX}{CQLSH} -e 'DESCRIBE KEYSPACES;' | grep gaming_recommender"
        ),
    )

    validate_datasets = BashOperator(
        task_id="validate_kaggle_datasets",
        bash_command=f"{ENV_PREFIX}{PYTHON} {PROJECT_DIR}/0_data/kaggle/validate_datasets.py",
    )

    run_kdd = BashOperator(
        task_id="spark_kdd_recommender_pipeline",
        bash_command=f"{ENV_PREFIX}bash {PROJECT_DIR}/4_batch_layer/submit_recommender_kdd.sh --date {{{{ ds }}}} --alpha 0.7",
    )

    verify_cassandra = BashOperator(
        task_id="verify_recommendations_written",
        bash_command=r"""
set -e
COUNT=$(cqlsh -e "SELECT COUNT(*) FROM gaming_recommender.user_recommendations;" | grep -oP '\d+' | tail -1)
echo "Filas user_recommendations: $COUNT"
test "${COUNT:-0}" -gt 0
""",
    )

    [check_kafka, check_cassandra, validate_datasets] >> run_kdd >> verify_cassandra
