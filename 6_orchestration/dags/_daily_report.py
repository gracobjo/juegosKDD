#!/usr/bin/env python3
"""
_daily_report.py — invocado por la tarea `daily_report` del DAG.
Lee game_stats_daily de Cassandra para la fecha indicada por la variable de
entorno DT (YYYY-MM-DD) y lo imprime formateado en stdout.
"""

import os
import sys

from cassandra.cluster import Cluster


def main() -> int:
    dt = os.environ.get("DT")
    if not dt:
        print("ERROR: falta variable DT (YYYY-MM-DD)")
        return 1

    cluster = Cluster(["localhost"])
    session = cluster.connect("gaming_kdd")

    rows = list(
        session.execute(
            "SELECT game, avg_players, avg_review_score, growth_rate, player_rank "
            "FROM game_stats_daily WHERE dt = %s ALLOW FILTERING",
            (dt,),
        )
    )

    print(f"\n=== KDD Daily Report — {dt} ===")
    print(f"{'Rank':<5} {'Juego':<30} {'Jugadores':<12} {'Reviews':<10} Growth")
    print("-" * 72)
    for r in sorted(rows, key=lambda x: x.player_rank or 99):
        growth_val = r.growth_rate or 0
        growth_str = f"+{growth_val:.1f}%" if growth_val >= 0 else f"{growth_val:.1f}%"
        print(
            f"{r.player_rank or 0:<5} "
            f"{(r.game or '—'):<30} "
            f"{int(r.avg_players or 0):<12,} "
            f"{(r.avg_review_score or 0):<10.1f} "
            f"{growth_str}"
        )

    cluster.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
