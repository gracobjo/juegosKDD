"""
Helper de I/O Hive para el pipeline del recomendador.

Sortea la incompatibilidad entre el cliente Hive 2.3 embebido en Spark y el
Hive Metastore 4.2.0 externo (error 'Invalid method name: get_table'):
  - Lectura: spark.read.parquet(HDFS_PATH)
  - Escritura: df.write.parquet(HDFS_PATH) + CREATE EXTERNAL TABLE via beeline
"""

from __future__ import annotations

import logging
import subprocess
from textwrap import dedent
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructType,
    TimestampType,
)

log = logging.getLogger(__name__)

DATABASE = "gaming_recommender"
HDFS_BASE = f"hdfs://nodo1:9000/user/hive/warehouse/{DATABASE}.db"
BEELINE = "/home/hadoop/apache-hive-4.2.0-bin/bin/beeline"
HS2_URL = "jdbc:hive2://localhost:10000/default"


_SPARK_TO_HIVE = {
    StringType: "STRING",
    IntegerType: "INT",
    LongType: "BIGINT",
    ShortType: "SMALLINT",
    FloatType: "FLOAT",
    DoubleType: "DOUBLE",
    BooleanType: "BOOLEAN",
    DateType: "DATE",
    TimestampType: "TIMESTAMP",
}


def table_path(table: str) -> str:
    return f"{HDFS_BASE}/{table}"


def _spark_dtype_to_hive(dtype) -> str:
    name = type(dtype).__name__
    for spark_t, hive_t in _SPARK_TO_HIVE.items():
        if name == spark_t.__name__:
            return hive_t
    if name == "ArrayType":
        inner = _spark_dtype_to_hive(dtype.elementType)
        return f"ARRAY<{inner}>"
    if name == "DecimalType":
        return f"DECIMAL({dtype.precision},{dtype.scale})"
    log.warning("Tipo Spark no mapeado %s -> STRING", name)
    return "STRING"


def _ddl_from_schema(schema: StructType) -> str:
    cols = []
    for field in schema.fields:
        cols.append(f"  `{field.name}` {_spark_dtype_to_hive(field.dataType)}")
    return ",\n".join(cols)


def beeline_exec(sql: str) -> None:
    log.info("beeline: %s", sql.strip().splitlines()[0][:120])
    cmd = [BEELINE, "-u", HS2_URL, "-n", "hadoop", "--silent=true", "-e", sql]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        log.error("beeline stderr: %s", proc.stderr.strip()[-2000:])
        raise RuntimeError(f"beeline fallo (rc={proc.returncode})")


def read_table(spark: SparkSession, table: str) -> DataFrame:
    """Lee una tabla del recomendador desde su Parquet en HDFS."""
    path = table_path(table)
    return spark.read.parquet(path)


def write_table(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    *,
    mode: str = "overwrite",
    partition_by: Optional[list] = None,
    register_external: bool = True,
) -> str:
    """
    Persiste un DataFrame como Parquet en HDFS y registra (opcional) tabla EXTERNAL en Hive 4.2.
    Devuelve la ruta HDFS.
    """
    path = table_path(table)
    log.info("Escribiendo Parquet %s -> %s (mode=%s)", table, path, mode)

    writer = (
        df.write.mode(mode)
        .option("compression", "snappy")
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)

    if register_external:
        ddl = _ddl_from_schema(df.schema if not partition_by else
                               StructType([f for f in df.schema.fields if f.name not in partition_by]))
        partition_clause = ""
        if partition_by:
            part_cols = []
            for f in df.schema.fields:
                if f.name in partition_by:
                    part_cols.append(f"`{f.name}` {_spark_dtype_to_hive(f.dataType)}")
            partition_clause = f"PARTITIONED BY ({', '.join(part_cols)})\n"
        sql = dedent(f"""
            CREATE DATABASE IF NOT EXISTS {DATABASE};
            DROP TABLE IF EXISTS {DATABASE}.{table};
            CREATE EXTERNAL TABLE {DATABASE}.{table} (
            {ddl}
            )
            {partition_clause}STORED AS PARQUET
            LOCATION '{path}';
        """).strip()
        beeline_exec(sql)
        if partition_by:
            beeline_exec(f"MSCK REPAIR TABLE {DATABASE}.{table};")
    return path


def table_exists(spark: SparkSession, table: str) -> bool:
    """Verifica si la tabla tiene datos en HDFS (más rápido que Hive metastore)."""
    try:
        spark.read.parquet(table_path(table)).limit(1).count()
        return True
    except Exception:
        return False
