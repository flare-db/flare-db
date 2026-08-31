import sys
import os

LOCAL_PATH = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, LOCAL_PATH)

try:
    # Changed from apache_beam to flaredb_beam
    from flaredb_beam.runners.portability.flaredb_runner import FlareDBRunner
    print("✅ Success: SDK Module Imports Successfully!")
except ModuleNotFoundError as e:
    print(f"❌ Diagnostic Fail: {e}")
