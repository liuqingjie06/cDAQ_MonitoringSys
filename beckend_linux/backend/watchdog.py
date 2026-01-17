import subprocess
import sys
import time
from pathlib import Path


def main():
    app_path = Path(__file__).parent / "app.py"
    if not app_path.exists():
        print("app.py not found.")
        sys.exit(1)

    while True:
        try:
            print("[watchdog] starting backend...")
            proc = subprocess.Popen([sys.executable, str(app_path)])
            proc.wait()
            code = proc.returncode
            print(f"[watchdog] backend exited with code {code}, restarting in 2s...")
            time.sleep(2)
        except KeyboardInterrupt:
            print("[watchdog] stopped by user.")
            break
        except Exception as e:
            print(f"[watchdog] error: {e}, retrying in 5s...")
            time.sleep(5)


if __name__ == "__main__":
    main()
