from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[3]

print(PROJECT_ROOT)

sys.path.append(str(PROJECT_ROOT))