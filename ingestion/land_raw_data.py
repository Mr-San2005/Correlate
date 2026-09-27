import subprocess
from pathlib import Path
from datetime import date

LOCAL_DIR = Path("data_generator/output")
CONTAINER = "namenode"
INGESTION_DATE = date.today().isoformat()

SOURCES = {
    "sales_daily.csv": "sales",
    "web_traffic_daily.csv": "web_traffic",
    "app_performance_daily.csv": "app_performance",
    "marketing_spend_daily.csv": "marketing",
    "customers_daily.csv": "customers",
    "ops_events.csv": "ops_events",
}


def run(cmd):
    print("$ " + " ".join(cmd))
    subprocess.run(cmd, check=True)


def main():
    for filename, source in SOURCES.items():
        local_path = LOCAL_DIR / filename
        container_tmp_path = f"/tmp/{filename}"
        hdfs_dir = f"/raw/{source}/ingestion_date={INGESTION_DATE}"

        # 1. copy the file from your laptop into the namenode container
        run(["docker", "cp", str(local_path), f"{CONTAINER}:{container_tmp_path}"])

        # 2. create the target folder inside HDFS (if it doesn't exist yet)
        run(["docker", "exec", CONTAINER, "hdfs", "dfs", "-mkdir", "-p", hdfs_dir])

        # 3. move the file from inside the container into HDFS
        run(["docker", "exec", CONTAINER, "hdfs", "dfs", "-put", "-f",
             container_tmp_path, f"{hdfs_dir}/{filename}"])

        print(f"landed {filename} -> hdfs:{hdfs_dir}/{filename}\n")


if __name__ == "__main__":
    main()