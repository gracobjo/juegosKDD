#!/usr/bin/env python3
"""
Herramienta Cassandra sin depender del cqlsh del tarball (roto en Python 3.12:
asyncore eliminado, extensiones libev).

  Aplicar DDL:
    source venv/bin/activate
    python3 0_infra/apply_cassandra_schema.py
    python3 0_infra/apply_cassandra_schema.py /ruta/schema.cql

  Comprobar keyspace (útil en scripts bash):
    python3 0_infra/apply_cassandra_schema.py --exists gaming_recommender

  Listar tablas de un keyspace:
    python3 0_infra/apply_cassandra_schema.py --tables gaming_kdd

Variables: CASSANDRA_HOST (localhost), CASSANDRA_PORT (9042)
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CQL = ROOT / "5_serving_layer" / "cassandra_schema.cql"


def strip_line_comment(line: str) -> str:
    line = re.sub(r"\s--.*$", "", line)
    return line.rstrip()


def split_statements(cql: str) -> list[str]:
    lines_out: list[str] = []
    for line in cql.splitlines():
        raw = line.strip()
        if not raw or raw.startswith("--"):
            continue
        lines_out.append(strip_line_comment(line))
    blob = "\n".join(lines_out)
    parts: list[str] = []
    for chunk in blob.split(";"):
        s = chunk.strip()
        if s:
            parts.append(s)
    return parts


def _connect():
    from cassandra.cluster import Cluster

    host = os.getenv("CASSANDRA_HOST", "localhost")
    port = int(os.getenv("CASSANDRA_PORT", "9042"))
    cluster = Cluster([host], port=port)
    return cluster, cluster.connect()


def cmd_exists(keyspace: str) -> int:
    from cassandra.cluster import Cluster

    host = os.getenv("CASSANDRA_HOST", "localhost")
    port = int(os.getenv("CASSANDRA_PORT", "9042"))
    cluster = Cluster([host], port=port)
    session = cluster.connect()
    rows = list(
        session.execute(
            "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = %s",
            (keyspace,),
        )
    )
    cluster.shutdown()
    return 0 if rows else 1


def cmd_tables(keyspace: str) -> int:
    cluster, session = _connect()
    rows = session.execute(
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s",
        (keyspace,),
    )
    for r in rows:
        print(f"    {r.table_name}")
    cluster.shutdown()
    return 0


def cmd_list_keyspaces() -> int:
    cluster, session = _connect()
    rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
    for r in rows:
        print(f"    {r.keyspace_name}")
    cluster.shutdown()
    return 0


def cmd_apply(path: Path) -> int:
    if not path.is_file():
        print(f"No existe el fichero: {path}", file=sys.stderr)
        return 1

    try:
        from cassandra.cluster import Cluster  # noqa: F401
    except ImportError:
        print(
            "Falta cassandra-driver. Activa el venv: source venv/bin/activate",
            file=sys.stderr,
        )
        return 1

    text = path.read_text(encoding="utf-8")
    statements = split_statements(text)
    if not statements:
        print("No se encontraron sentencias CQL.", file=sys.stderr)
        return 1

    cluster, session = _connect()
    host = os.getenv("CASSANDRA_HOST", "localhost")
    port = int(os.getenv("CASSANDRA_PORT", "9042"))
    print(f"Conectado a {host}:{port} — {len(statements)} sentencias desde {path.name}")
    for i, stmt in enumerate(statements, 1):
        preview = stmt.replace("\n", " ")[:72]
        try:
            session.execute(stmt)
            print(f"  [{i}/{len(statements)}] OK — {preview}…")
        except Exception as e:
            print(f"  [{i}/{len(statements)}] ERROR — {preview}…\n    {e}", file=sys.stderr)
            cluster.shutdown()
            return 1

    cluster.shutdown()
    print("Esquema aplicado correctamente.")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="DDL Cassandra sin cqlsh del tarball")
    p.add_argument("cql_file", nargs="?", type=Path, help="Fichero .cql (por defecto cassandra_schema.cql)")
    p.add_argument("--exists", metavar="KEYSPACE", help="Exit 0 si el keyspace existe")
    p.add_argument("--tables", metavar="KEYSPACE", help="Listar tablas del keyspace")
    p.add_argument("--list-keyspaces", action="store_true", help="Listar todos los keyspaces")
    args = p.parse_args()

    if args.list_keyspaces:
        return cmd_list_keyspaces()
    if args.exists:
        return cmd_exists(args.exists)
    if args.tables:
        return cmd_tables(args.tables)

    path = args.cql_file if args.cql_file else DEFAULT_CQL
    return cmd_apply(path)


if __name__ == "__main__":
    sys.exit(main())
