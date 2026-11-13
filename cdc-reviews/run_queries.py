"""분석 SQL의 결과와 조회 시간을 비교한다."""

import difflib
import json
import time

from common import LOG_DIR, PROJECT_ROOT, dw

FILES = [
    PROJECT_ROOT / path
    for path in ["queries/q1_sale.sql", "queries/q2_dry_low.sql", "queries/q3_ai_changed.sql"]
]


def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    base = open(FILES[0], encoding="utf-8").read().splitlines()
    out = []
    with dw() as c, c.cursor() as cur:
        for path in FILES:
            sql = open(path, encoding="utf-8").read()
            lines = sql.splitlines()
            diff = [d for d in difflib.ndiff(base, lines) if d[:1] in "+-"]
            changed = max(sum(1 for d in diff if d[0] == "+"), sum(1 for d in diff if d[0] == "-"))
            times = []
            for _ in range(5):
                t = time.perf_counter()
                cur.execute(sql)
                rows = cur.fetchall()
                times.append((time.perf_counter() - t) * 1000)
            out.append(
                {
                    "query": path.name,
                    "lines_changed_vs_q1": changed,
                    "median_ms": round(sorted(times)[2], 1),
                    "rows": rows,
                }
            )
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    with open((LOG_DIR / "queries.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=str)


if __name__ == "__main__":
    main()
