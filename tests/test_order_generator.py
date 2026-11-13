import csv
import subprocess
import sys
from pathlib import Path


def test_generated_orders_reference_customers_and_date_partitions(tmp_path):
    script = Path(__file__).resolve().parents[1] / "batch-orders/scripts/generate_orders.py"
    subprocess.run(
        [
            sys.executable,
            str(script),
            "--people",
            "20",
            "--orders",
            "100",
            "--days",
            "3",
            "--out",
            str(tmp_path),
        ],
        check=True,
        capture_output=True,
    )
    files = list((tmp_path / "orders").rglob("*.csv"))
    assert files
    total = 0
    for path in files:
        brand = path.parent.parent.name
        with (tmp_path / "customers" / brand / "customers.csv").open(encoding="utf-8") as file:
            customers = {row["brand_customer_id"] for row in csv.DictReader(file)}
        with path.open(encoding="utf-8") as file:
            orders = list(csv.DictReader(file))
        assert all(row["brand_customer_id"] in customers for row in orders)
        assert all(
            row["order_ts"].split()[0] == path.parent.name.removeprefix("dt=") for row in orders
        )
        total += len(orders)
    assert 98 <= total <= 100
