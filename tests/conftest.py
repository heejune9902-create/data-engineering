import os
import sys
from pathlib import Path

os.environ["POSTGRES_PASSWORD"] = "unused-test-password"
os.environ["PSEUDONYM_KEY"] = "unused-test-key"
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "cdc-reviews"))
