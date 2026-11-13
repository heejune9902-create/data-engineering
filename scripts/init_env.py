"""로컬 실행용 환경 파일을 만들고 기존 파일은 유지한다."""

import base64
import secrets
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    for folder in (root / "batch-orders", root / "batch-orders/airflow", root / "cdc-reviews"):
        target = folder / ".env"
        if target.exists():
            print(f"[INFO] 기존 설정 유지: {target.relative_to(root)}")
            continue
        values = []
        for line in (folder / ".env.example").read_text(encoding="utf-8").splitlines():
            key, value = line.split("=", 1)
            if key == "BATCH_PROJECT_DIR":
                value = (root / "batch-orders").as_posix()
            elif key == "FERNET_KEY":
                value = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii")
            elif value.startswith("replace-with-"):
                value = secrets.token_urlsafe(32)
            values.append(f"{key}={value}")
        target.write_text("\n".join(values) + "\n", encoding="utf-8")
        print(f"[INFO] 설정 생성: {target.relative_to(root)}")


if __name__ == "__main__":
    main()
