import pandas as pd
from pathlib import Path

OUTPUT_DIR = Path("data_generator/output")


def test_files_exist():
    expected_files = [
        "sales_daily.csv",
        "web_traffic_daily.csv",
        "app_performance_daily.csv",
        "marketing_spend_daily.csv",
        "customers_daily.csv",
        "ops_events.csv",
    ]
    for f in expected_files:
        assert (OUTPUT_DIR / f).exists(), f"{f} was not generated"


def test_sales_has_expected_columns():
    df = pd.read_csv(OUTPUT_DIR / "sales_daily.csv")
    assert {"date", "orders", "revenue", "ingestion_date"}.issubset(df.columns)


def test_anomaly_is_visible_in_revenue():
    df = pd.read_csv(OUTPUT_DIR / "sales_daily.csv", parse_dates=["date"])
    df = df.dropna(subset=["revenue"]).drop_duplicates(subset=["date"])
    df = df.sort_values("date").reset_index(drop=True)

    anomaly_day = df.iloc[200]["revenue"]
    normal_days_avg = df.iloc[190:199]["revenue"].mean()

    assert anomaly_day < normal_days_avg * 0.85