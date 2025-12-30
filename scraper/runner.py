import json
import os
import sys
import shutil
import traceback
from datetime import datetime
from bypass_parallel import main

def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def finalize_job(job_path, status):
    base_dir = os.path.dirname(os.path.dirname(job_path))
    target_dir = os.path.join(base_dir, status)

    os.makedirs(target_dir, exist_ok=True)

    filename = os.path.basename(job_path)
    target_path = os.path.join(target_dir, filename)

    shutil.move(job_path, target_path)


def setup_logging(job_path):
    # หา root directory ของโปรเจค (ขึ้นไป 3 ระดับจาก jobs/running/job.json)
    # หรือใช้ตำแหน่งของ runner.py เป็นฐาน
    runner_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(runner_dir)  # ขึ้นไป 1 ระดับจาก scraper/ -> root
    logs_dir = os.path.join(base_dir, "logs", "jobs")
    os.makedirs(logs_dir, exist_ok=True)

    job_name = os.path.splitext(os.path.basename(job_path))[0]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    log_path = os.path.join(logs_dir, f"{job_name}_{timestamp}.log")

    sys.stdout = open(log_path, "w", encoding="utf-8")
    sys.stderr = sys.stdout

    print(f"[LOG] Logging to {log_path}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        # If no argument provided, try to find a pending job for testing
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        pending_dir = os.path.join(base_dir, "jobs", "pending")
        
        if os.path.exists(pending_dir):
            pending_jobs = [f for f in os.listdir(pending_dir) if f.endswith(".json")]
            if pending_jobs:
                job_path = os.path.join(pending_dir, pending_jobs[0])
                print(f"[RUNNER] No job path provided, using first pending job: {job_path}")
            else:
                print("[ERROR] No job path provided and no pending jobs found.")
                print("Usage: python runner.py <job_path>")
                sys.exit(1)
        else:
            print("[ERROR] No job path provided.")
            print("Usage: python runner.py <job_path>")
            sys.exit(1)
    else:
        job_path = sys.argv[1]

    setup_logging(job_path)

    try:
        print(f"[RUNNER] Job started: {job_path}")

        cfg = load_config(job_path)

        main(
            sites=cfg["sites"],
            visits_per_site=int(cfg["visits_per_site"]),
            max_workers=int(cfg["max_workers"])
        )

        finalize_job(job_path, "done")
        print("[RUNNER] Job finished → done")

    except Exception:
        print("[RUNNER] Job failed")
        traceback.print_exc()
        finalize_job(job_path, "failed")
        raise

