"""환경 설정을 적용해 Debezium 커넥터를 등록한다."""

import json
import os
import urllib.request
from pathlib import Path

from dotenv import load_dotenv


def main():
    root = Path(__file__).resolve().parent
    load_dotenv(root / ".env")
    connector = json.loads((root / "connector.json").read_text(encoding="utf-8"))
    connector["config"]["database.password"] = os.environ["POSTGRES_PASSWORD"]
    endpoint = os.environ.get("CONNECT_URL", "http://localhost:8083").rstrip("/")
    request = urllib.request.Request(
        f"{endpoint}/connectors/{connector['name']}/config",
        data=json.dumps(connector["config"]).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        result = json.load(response)
    print(f"[INFO] 커넥터 등록 완료: {result['name']}")


if __name__ == "__main__":
    main()
