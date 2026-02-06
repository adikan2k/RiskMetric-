"""
RiskMetric — Bronze Layer: Synthetic Transaction Generator
Generates 1M+ banking transactions with injected fraud archetypes:
  1. Impossible Travel (transactions in distant cities within minutes)
  2. Velocity Spikes (10+ micro-transactions in < 60 seconds)
  3. Behavioral Drift (amounts > 3σ from user's normal spending)
"""

import os
import uuid
import random
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "raw_transactions.parquet")
NUM_USERS = 5_000
NUM_TRANSACTIONS = 1_050_000
FRAUD_IMPOSSIBLE_TRAVEL = 2_500
FRAUD_VELOCITY_SPIKE_USERS = 500
FRAUD_BEHAVIORAL_DRIFT = 3_000

CITIES = [
    {"city": "New York",      "country": "US", "lat": 40.7128,  "lon": -74.0060},
    {"city": "London",        "country": "GB", "lat": 51.5074,  "lon": -0.1278},
    {"city": "Tokyo",         "country": "JP", "lat": 35.6762,  "lon": 139.6503},
    {"city": "Sydney",        "country": "AU", "lat": -33.8688, "lon": 151.2093},
    {"city": "São Paulo",     "country": "BR", "lat": -23.5505, "lon": -46.6333},
    {"city": "Mumbai",        "country": "IN", "lat": 19.0760,  "lon": 72.8777},
    {"city": "Dubai",         "country": "AE", "lat": 25.2048,  "lon": 55.2708},
    {"city": "Singapore",     "country": "SG", "lat": 1.3521,   "lon": 103.8198},
    {"city": "Toronto",       "country": "CA", "lat": 43.6532,  "lon": -79.3832},
    {"city": "Berlin",        "country": "DE", "lat": 52.5200,  "lon": 13.4050},
    {"city": "Paris",         "country": "FR", "lat": 48.8566,  "lon": 2.3522},
    {"city": "Lagos",         "country": "NG", "lat": 6.5244,   "lon": 3.3792},
    {"city": "Mexico City",   "country": "MX", "lat": 19.4326,  "lon": -99.1332},
    {"city": "Seoul",         "country": "KR", "lat": 37.5665,  "lon": 126.9780},
    {"city": "Chicago",       "country": "US", "lat": 41.8781,  "lon": -87.6298},
    {"city": "Los Angeles",   "country": "US", "lat": 34.0522,  "lon": -118.2437},
    {"city": "Shanghai",      "country": "CN", "lat": 31.2304,  "lon": 121.4737},
    {"city": "Moscow",        "country": "RU", "lat": 55.7558,  "lon": 37.6173},
    {"city": "Cairo",         "country": "EG", "lat": 30.0444,  "lon": 31.2357},
    {"city": "Istanbul",      "country": "TR", "lat": 41.0082,  "lon": 28.9784},
]

DISTANT_PAIRS = [
    (0, 1),   # New York <-> London
    (0, 2),   # New York <-> Tokyo
    (1, 3),   # London <-> Sydney
    (2, 4),   # Tokyo <-> São Paulo
    (5, 6),   # Mumbai <-> Dubai (closer but still flaggable with short time)
    (7, 3),   # Singapore <-> Sydney
    (0, 15),  # New York <-> Los Angeles
    (1, 16),  # London <-> Shanghai
]

MERCHANT_CATEGORIES = [
    "Grocery", "Restaurant", "Gas Station", "Online Shopping",
    "Electronics", "Travel", "Entertainment", "Healthcare",
    "Utilities", "Clothing", "Hotel", "ATM Withdrawal",
    "Wire Transfer", "Subscription", "Insurance",
]


def generate_user_profiles(n_users: int) -> pd.DataFrame:
    """Generate user profiles with home city and spending habits."""
    users = []
    for i in range(n_users):
        home = random.choice(CITIES)
        avg_spend = round(random.uniform(20, 500), 2)
        std_spend = round(avg_spend * random.uniform(0.15, 0.40), 2)
        users.append({
            "user_id": f"USR-{i:06d}",
            "home_city": home["city"],
            "home_country": home["country"],
            "home_lat": home["lat"],
            "home_lon": home["lon"],
            "avg_amount": avg_spend,
            "std_amount": std_spend,
        })
    return pd.DataFrame(users)


def generate_legitimate_transactions(users: pd.DataFrame, n_txns: int) -> list[dict]:
    """Generate normal, legitimate transactions."""
    print(f"  Generating {n_txns:,} legitimate transactions...")
    txns = []
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 12, 31)
    delta_seconds = int((end_date - start_date).total_seconds())

    for _ in range(n_txns):
        user = users.sample(1).iloc[0]
        jitter_lat = user["home_lat"] + random.uniform(-0.5, 0.5)
        jitter_lon = user["home_lon"] + random.uniform(-0.5, 0.5)
        amount = max(0.50, round(np.random.normal(user["avg_amount"], user["std_amount"]), 2))
        ts = start_date + timedelta(seconds=random.randint(0, delta_seconds))

        txns.append({
            "transaction_id": str(uuid.uuid4()),
            "user_id": user["user_id"],
            "timestamp": ts,
            "amount": amount,
            "merchant_name": fake.company(),
            "merchant_category": random.choice(MERCHANT_CATEGORIES),
            "city": user["home_city"],
            "country": user["home_country"],
            "latitude": round(jitter_lat, 6),
            "longitude": round(jitter_lon, 6),
            "is_fraud": False,
            "fraud_type": None,
        })
    return txns


def inject_impossible_travel(users: pd.DataFrame, n_pairs: int) -> list[dict]:
    """Inject Impossible Travel fraud: two transactions in distant cities within 10 min."""
    print(f"  Injecting {n_pairs:,} impossible-travel fraud pairs...")
    txns = []
    sampled_users = users.sample(n=min(n_pairs, len(users)), replace=True)

    for _, user in sampled_users.iterrows():
        pair = random.choice(DISTANT_PAIRS)
        city_a = CITIES[pair[0]]
        city_b = CITIES[pair[1]]
        base_ts = datetime(2025, 1, 1) + timedelta(
            seconds=random.randint(0, int(timedelta(days=364).total_seconds()))
        )
        gap_minutes = random.randint(2, 10)

        for idx, city in enumerate([city_a, city_b]):
            ts = base_ts + timedelta(minutes=idx * gap_minutes)
            txns.append({
                "transaction_id": str(uuid.uuid4()),
                "user_id": user["user_id"],
                "timestamp": ts,
                "amount": round(random.uniform(10, 2000), 2),
                "merchant_name": fake.company(),
                "merchant_category": random.choice(MERCHANT_CATEGORIES),
                "city": city["city"],
                "country": city["country"],
                "latitude": round(city["lat"] + random.uniform(-0.01, 0.01), 6),
                "longitude": round(city["lon"] + random.uniform(-0.01, 0.01), 6),
                "is_fraud": True,
                "fraud_type": "impossible_travel",
            })
    return txns


def inject_velocity_spikes(users: pd.DataFrame, n_users: int) -> list[dict]:
    """Inject Velocity Spike fraud: 10+ micro-transactions in < 60 seconds (card testing)."""
    print(f"  Injecting velocity-spike fraud for {n_users:,} users...")
    txns = []
    sampled_users = users.sample(n=min(n_users, len(users)), replace=False)

    for _, user in sampled_users.iterrows():
        base_ts = datetime(2025, 1, 1) + timedelta(
            seconds=random.randint(0, int(timedelta(days=364).total_seconds()))
        )
        n_micro = random.randint(10, 20)
        city_info = random.choice(CITIES)

        for i in range(n_micro):
            ts = base_ts + timedelta(seconds=random.randint(0, 55))
            txns.append({
                "transaction_id": str(uuid.uuid4()),
                "user_id": user["user_id"],
                "timestamp": ts,
                "amount": round(random.uniform(0.01, 2.00), 2),
                "merchant_name": fake.company(),
                "merchant_category": "Online Shopping",
                "city": city_info["city"],
                "country": city_info["country"],
                "latitude": round(city_info["lat"] + random.uniform(-0.01, 0.01), 6),
                "longitude": round(city_info["lon"] + random.uniform(-0.01, 0.01), 6),
                "is_fraud": True,
                "fraud_type": "velocity_spike",
            })
    return txns


def inject_behavioral_drift(users: pd.DataFrame, n_txns: int) -> list[dict]:
    """Inject Behavioral Drift fraud: amounts > 3σ from user's normal spending."""
    print(f"  Injecting {n_txns:,} behavioral-drift fraud transactions...")
    txns = []
    sampled_users = users.sample(n=min(n_txns, len(users)), replace=True)

    for _, user in sampled_users.iterrows():
        sigma_multiplier = random.uniform(3.5, 8.0)
        drift_amount = round(user["avg_amount"] + sigma_multiplier * user["std_amount"], 2)
        drift_amount = max(drift_amount, 500.0)
        ts = datetime(2025, 1, 1) + timedelta(
            seconds=random.randint(0, int(timedelta(days=364).total_seconds()))
        )

        txns.append({
            "transaction_id": str(uuid.uuid4()),
            "user_id": user["user_id"],
            "timestamp": ts,
            "amount": drift_amount,
            "merchant_name": fake.company(),
            "merchant_category": random.choice(["Wire Transfer", "Electronics", "Travel", "Online Shopping"]),
            "city": user["home_city"],
            "country": user["home_country"],
            "latitude": round(user["home_lat"] + random.uniform(-0.3, 0.3), 6),
            "longitude": round(user["home_lon"] + random.uniform(-0.3, 0.3), 6),
            "is_fraud": True,
            "fraud_type": "behavioral_drift",
        })
    return txns


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("=" * 60)
    print("RiskMetric — Bronze Layer Generator")
    print("=" * 60)

    print("\n[1/5] Generating user profiles...")
    users = generate_user_profiles(NUM_USERS)
    users_file = os.path.join(OUTPUT_DIR, "user_profiles.parquet")
    users.to_parquet(users_file, engine="pyarrow", index=False)
    print(f"  Saved {len(users):,} user profiles -> {users_file}")

    print("\n[2/5] Generating legitimate transactions...")
    legit = generate_legitimate_transactions(users, NUM_TRANSACTIONS)

    print("\n[3/5] Injecting fraud archetypes...")
    impossible = inject_impossible_travel(users, FRAUD_IMPOSSIBLE_TRAVEL)
    velocity = inject_velocity_spikes(users, FRAUD_VELOCITY_SPIKE_USERS)
    drift = inject_behavioral_drift(users, FRAUD_BEHAVIORAL_DRIFT)

    all_txns = legit + impossible + velocity + drift
    random.shuffle(all_txns)

    print(f"\n[4/5] Building DataFrame ({len(all_txns):,} total records)...")
    df = pd.DataFrame(all_txns)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(["user_id", "timestamp"]).reset_index(drop=True)

    print(f"\n[5/5] Writing to Parquet -> {OUTPUT_FILE}")
    df.to_parquet(OUTPUT_FILE, engine="pyarrow", index=False)

    print("\n" + "=" * 60)
    print("GENERATION COMPLETE")
    print("=" * 60)
    print(f"  Total transactions : {len(df):,}")
    print(f"  Legitimate         : {len(legit):,}")
    print(f"  Impossible Travel  : {len(impossible):,}")
    print(f"  Velocity Spikes    : {len(velocity):,}")
    print(f"  Behavioral Drift   : {len(drift):,}")
    print(f"  Fraud rate         : {df['is_fraud'].mean():.2%}")
    print(f"  Date range         : {df['timestamp'].min()} → {df['timestamp'].max()}")
    print(f"  File size          : {os.path.getsize(OUTPUT_FILE) / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
