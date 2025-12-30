import os
import time
import json
import subprocess
import shutil
from datetime import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
JOBS_DIR = os.path.join(BASE_DIR, "jobs", "pending")
RUNNER_PATH = os.path.join(BASE_DIR, "scraper", "runner.py")
PYTHON_EXE = os.path.join(BASE_DIR, ".venv", "Scripts", "python.exe")
JOBS_BASE = os.path.join(BASE_DIR, "jobs")
PENDING_DIR = os.path.join(JOBS_BASE, "pending")
RUNNING_DIR = os.path.join(JOBS_BASE, "running")
DONE_DIR = os.path.join(JOBS_BASE, "done")


CHECK_INTERVAL = 10  # seconds

def load_job(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_job(job_path):
    filename = os.path.basename(job_path)
    running_path = os.path.join(RUNNING_DIR, filename)

    shutil.move(job_path, running_path)

    print(f"[SCHEDULER] Launching job: {filename}")

    subprocess.Popen([
        PYTHON_EXE,
        RUNNER_PATH,
        running_path
    ])
    

def recover_running_jobs():
    for filename in os.listdir(RUNNING_DIR):
        src = os.path.join(RUNNING_DIR, filename)
        dst = os.path.join(PENDING_DIR, filename)
        shutil.move(src, dst)
        print(f"[RECOVERY] Returned {filename} to pending")

def parse_run_at(value):
    return datetime.strptime(value, "%Y-%m-%d %H:%M")

def should_run(job):
    if "run_at" not in job:
        return True  # backward compatible

    run_at = parse_run_at(job["run_at"])
    now = datetime.now()

    return now >= run_at


def main():
    print("[SCHEDULER] Service started")

    # print("[SCHEDULER] Recovering running jobs")
    # recover_running_jobs()

    print("[SCHEDULER] Watching:", JOBS_DIR)

    while True:
        try:
            for filename in os.listdir(JOBS_DIR):
                if not filename.endswith(".json"):
                    continue

                job_path = os.path.join(PENDING_DIR, filename)

                try:
                    job = load_job(job_path)
                except Exception as e:
                    print(f"[ERROR] Failed to load {filename}: {e}")
                    continue

                if should_run(job):
                    run_job(job_path)
                    print("[bypass_parallel] finished")
                    # 🚨 IMPORTANT: minimal mode → ลบ job ทิ้งก่อน
                    # (ป้องกันรันซ้ำ)
                    # os.remove(job_path)
                    # print(f"[SCHEDULER] Job removed: {filename}")

        except Exception as e:
            print(f"[SCHEDULER] Loop error: {e}")

        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
