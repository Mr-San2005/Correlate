import subprocess
from datetime import date

CONTAINER = "namenode"
INGESTION_DATE = date.today().isoformat()

EXPECTED_PATHS = [
    f"/raw/sales/ingestion_date={INGESTION_DATE}/sales_daily.csv",
    f"/raw/web_traffic/ingestion_date={INGESTION_DATE}/web_traffic_daily.csv",
    f"/raw/app_performance/ingestion_date={INGESTION_DATE}/app_performance_daily.csv",
    f"/raw/marketing/ingestion_date={INGESTION_DATE}/marketing_spend_daily.csv",
    f"/raw/customers/ingestion_date={INGESTION_DATE}/customers_daily.csv",
    f"/raw/ops_events/ingestion_date={INGESTION_DATE}/ops_events.csv",
]


def test_files_exist_in_hdfs():
    for path in EXPECTED_PATHS:
        result = subprocess.run(
            ["docker", "exec", CONTAINER, "hdfs", "dfs", "-test", "-e", path]
        )
        assert result.returncode == 0, f"missing in HDFS: {path}"