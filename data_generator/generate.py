import numpy as np
import pandas as pd
from pathlib import Path

np.random.seed(42)  # keeps results the same every time we run this

START_DATE = "2025-01-01"
NUM_DAYS = 365
ANOMALY_DAY = 200  # the day something goes wrong (index into the date range)

OUTPUT_DIR = Path("data_generator/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

dates = pd.date_range(start=START_DATE, periods=NUM_DAYS, freq="D")


def weekly_seasonality(n):
    day_of_week = pd.date_range(start=START_DATE, periods=n, freq="D").dayofweek
    return np.where(day_of_week >= 5, 0.85, 1.0)  # weekends a bit quieter


def add_messiness(df, numeric_cols, null_rate=0.01, dup_rate=0.005, late_rate=0.02, max_late_days=5):
    df = df.copy()

    # some values go missing, like a sensor that failed to report that day
    for col in numeric_cols:
        mask = np.random.rand(len(df)) < null_rate
        df.loc[mask, col] = np.nan

    # a few rows get duplicated, like a source that sent the same record twice
    n_dupes = max(1, int(len(df) * dup_rate))
    dupe_rows = df.sample(n=n_dupes, replace=True)
    df = pd.concat([df, dupe_rows], ignore_index=True)

    # some records "arrive late" - the real event happened on `date`,
    # but the system only reported it a few days after
    df["ingestion_date"] = df["date"]
    late_mask = np.random.rand(len(df)) < late_rate
    late_days = np.random.randint(1, max_late_days + 1, size=len(df))
    df.loc[late_mask, "ingestion_date"] = df.loc[late_mask, "date"] + pd.to_timedelta(
        late_days[late_mask], unit="D"
    )

    return df


def generate_sales():
    trend = np.linspace(1.0, 1.15, NUM_DAYS)
    season = weekly_seasonality(NUM_DAYS)
    noise = np.random.normal(1.0, 0.05, NUM_DAYS)

    orders = 500 * trend * season * noise
    revenue = orders * np.random.normal(45, 3, NUM_DAYS)

    incident = slice(ANOMALY_DAY, ANOMALY_DAY + 3)
    orders[incident] *= 0.75    # orders drop 25%
    revenue[incident] *= 0.70   # revenue drops 30%

    df = pd.DataFrame({
        "date": dates,
        "orders": orders.round().astype(int),
        "revenue": revenue.round(2),
    })
    return add_messiness(df, ["orders", "revenue"])


def generate_traffic():
    trend = np.linspace(1.0, 1.1, NUM_DAYS)
    season = weekly_seasonality(NUM_DAYS)
    noise = np.random.normal(1.0, 0.06, NUM_DAYS)

    visits = 8000 * trend * season * noise
    unique_visitors = visits * np.random.normal(0.6, 0.03, NUM_DAYS)

    incident = slice(ANOMALY_DAY, ANOMALY_DAY + 3)
    visits[incident] *= 0.73          # traffic drops 27%
    unique_visitors[incident] *= 0.73

    df = pd.DataFrame({
        "date": dates,
        "visits": visits.round().astype(int),
        "unique_visitors": unique_visitors.round().astype(int),
    })
    return add_messiness(df, ["visits", "unique_visitors"])


def generate_performance():
    response_time = np.random.normal(220, 15, NUM_DAYS)  # milliseconds
    error_rate = np.random.normal(0.5, 0.1, NUM_DAYS)    # percent

    incident = slice(ANOMALY_DAY, ANOMALY_DAY + 3)
    response_time[incident] *= 4.0   # up ~300%
    error_rate[incident] *= 6.0      # up ~500%

    df = pd.DataFrame({
        "date": dates,
        "avg_response_time_ms": response_time.round(1),
        "error_rate_pct": error_rate.clip(min=0).round(3),
    })
    return add_messiness(df, ["avg_response_time_ms", "error_rate_pct"])


def generate_marketing():
    # spend stays steady through the incident on purpose - a real "unrelated" signal
    spend = np.random.normal(2000, 150, NUM_DAYS)
    df = pd.DataFrame({"date": dates, "spend": spend.round(2)})
    return add_messiness(df, ["spend"])


def generate_customers():
    new_customers = np.random.poisson(60, NUM_DAYS)
    churned = np.random.poisson(15, NUM_DAYS)

    incident = slice(ANOMALY_DAY, ANOMALY_DAY + 3)
    new_customers[incident] = (new_customers[incident] * 0.8).astype(int)

    df = pd.DataFrame({
        "date": dates,
        "new_customers": new_customers,
        "churned_customers": churned,
    })
    return add_messiness(df, ["new_customers", "churned_customers"])


def generate_ops_events():
    event_types = ["deploy", "incident", "config_change", "scheduled_maintenance"]
    rows = []
    for d in dates:
        for _ in range(np.random.poisson(1.2)):
            rows.append({
                "date": d,
                "event_type": np.random.choice(event_types, p=[0.5, 0.15, 0.3, 0.05]),
                "event_count": 1,
            })
    # make sure an "incident" is logged on the anomaly day itself
    rows.append({"date": dates[ANOMALY_DAY], "event_type": "incident", "event_count": 1})

    df = pd.DataFrame(rows)

    # every real event gets its own unique id, like a real ops log would have.
    # this is what lets us tell "sent twice" apart from "happened twice".
    df.insert(0, "event_id", [f"evt-{i:06d}" for i in range(1, len(df) + 1)])

    return add_messiness(df, ["event_count"], null_rate=0.0, dup_rate=0.02)


def main():
    datasets = {
        "sales_daily.csv": generate_sales(),
        "web_traffic_daily.csv": generate_traffic(),
        "app_performance_daily.csv": generate_performance(),
        "marketing_spend_daily.csv": generate_marketing(),
        "customers_daily.csv": generate_customers(),
        "ops_events.csv": generate_ops_events(),
    }
    for filename, df in datasets.items():
        path = OUTPUT_DIR / filename
        df.to_csv(path, index=False)
        print(f"wrote {len(df):5d} rows -> {path}")


if __name__ == "__main__":
    main()