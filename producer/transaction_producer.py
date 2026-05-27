"""
Transaction event producer.
Simulates a realistic e-commerce transaction stream at configurable throughput.
Sends events to Azure Event Hubs in batches for efficiency.

Usage:
    python transaction_producer.py --tps 500 --duration 300
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

from schema import MerchantCategory, TransactionEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
EVENTHUB_CONN_STR = os.environ["EVENTHUB_CONNECTION_STRING"]
EVENTHUB_NAME = os.environ.get("EVENTHUB_NAME", "transactions")

# Seed data pools (realistic distribution)
CUSTOMER_IDS = [f"CUST_{i:06d}" for i in range(1, 10_001)]   # 10K customers
MERCHANT_IDS = [f"MERCH_{i:04d}" for i in range(1, 1_001)]   # 1K merchants
COUNTRIES = ["US"] * 85 + ["CA"] * 5 + ["GB"] * 4 + ["MX"] * 3 + ["IN"] * 2 + ["AU"] * 1

CATEGORY_WEIGHTS = {
    MerchantCategory.GROCERY: 0.28,
    MerchantCategory.ONLINE: 0.22,
    MerchantCategory.DINING: 0.18,
    MerchantCategory.GAS: 0.12,
    MerchantCategory.ELECTRONICS: 0.08,
    MerchantCategory.TRAVEL: 0.06,
    MerchantCategory.ATM: 0.04,
    MerchantCategory.OTHER: 0.02,
}

# Amount distribution by category (mean, std)
AMOUNT_PARAMS = {
    MerchantCategory.GROCERY: (65.0, 30.0),
    MerchantCategory.ONLINE: (85.0, 60.0),
    MerchantCategory.DINING: (42.0, 25.0),
    MerchantCategory.GAS: (55.0, 20.0),
    MerchantCategory.ELECTRONICS: (320.0, 250.0),
    MerchantCategory.TRAVEL: (450.0, 400.0),
    MerchantCategory.ATM: (200.0, 100.0),
    MerchantCategory.OTHER: (75.0, 50.0),
}


def _generate_transaction(anomaly_rate: float = 0.02) -> TransactionEvent:
    """
    Generate a single synthetic transaction.
    anomaly_rate controls what fraction of events are intentionally anomalous
    (high amounts, foreign countries, card-not-present at unusual hours).
    """
    is_anomaly = random.random() < anomaly_rate
    category = random.choices(
        list(CATEGORY_WEIGHTS.keys()),
        weights=list(CATEGORY_WEIGHTS.values()),
        k=1,
    )[0]

    mean, std = AMOUNT_PARAMS[category]
    if is_anomaly:
        # Anomalies: 5–20x normal amount
        amount = round(abs(random.gauss(mean * random.uniform(5, 20), std * 3)), 2)
        country = random.choice(["NG", "RU", "VN", "UA"])  # high-risk countries
        card_present = False
    else:
        amount = round(max(1.0, random.gauss(mean, std)), 2)
        country = random.choice(COUNTRIES)
        card_present = random.random() > 0.35

    customer_id = random.choice(CUSTOMER_IDS)

    return TransactionEvent(
        customer_id=customer_id,
        account_id=f"ACC_{customer_id}_{random.randint(1, 3):01d}",
        amount=amount,
        merchant_id=random.choice(MERCHANT_IDS),
        merchant_category=category,
        merchant_country=country,
        card_present=card_present,
        transaction_ts=datetime.now(timezone.utc),
        ip_address=f"{random.randint(1,254)}.{random.randint(0,254)}.{random.randint(0,254)}.{random.randint(1,254)}",
        device_fingerprint=str(uuid.uuid4()).replace("-", "")[:32],
        latitude=round(random.uniform(25.0, 49.0), 6) if card_present else None,
        longitude=round(random.uniform(-125.0, -67.0), 6) if card_present else None,
    )


async def _send_batch(
    producer: EventHubProducerClient,
    events: list[TransactionEvent],
) -> int:
    """Send a list of transactions as a single Event Hub batch. Returns sent count."""
    batch = await producer.create_batch()
    sent = 0
    for event in events:
        try:
            batch.add(EventData(event.to_event_hub_bytes()))
            sent += 1
        except ValueError:
            # Batch full — flush and start new batch
            await producer.send_batch(batch)
            batch = await producer.create_batch()
            batch.add(EventData(event.to_event_hub_bytes()))
            sent += 1
    if len(batch) > 0:
        await producer.send_batch(batch)
    return sent


async def run(tps: int, duration_seconds: int, anomaly_rate: float) -> None:
    """
    Main producer loop.
    Targets `tps` transactions per second for `duration_seconds` seconds.
    Batches events every 100ms for efficient Event Hubs ingestion.
    """
    batch_interval = 0.1  # seconds between batches
    events_per_batch = max(1, int(tps * batch_interval))

    logger.info(
        "Starting producer | tps=%d | duration=%ds | batch_size=%d | anomaly_rate=%.1f%%",
        tps,
        duration_seconds,
        events_per_batch,
        anomaly_rate * 100,
    )

    async with EventHubProducerClient.from_connection_string(
        EVENTHUB_CONN_STR, eventhub_name=EVENTHUB_NAME
    ) as producer:
        total_sent = 0
        start_time = time.monotonic()
        deadline = start_time + duration_seconds

        while time.monotonic() < deadline:
            batch_start = time.monotonic()
            events = [_generate_transaction(anomaly_rate) for _ in range(events_per_batch)]
            sent = await _send_batch(producer, events)
            total_sent += sent

            elapsed = time.monotonic() - batch_start
            sleep_time = max(0.0, batch_interval - elapsed)
            await asyncio.sleep(sleep_time)

            if total_sent % 10_000 == 0:
                run_time = time.monotonic() - start_time
                actual_tps = total_sent / run_time if run_time > 0 else 0
                logger.info("Sent %d events | actual tps=%.0f", total_sent, actual_tps)

    total_time = time.monotonic() - start_time
    logger.info(
        "Producer finished | total_sent=%d | elapsed=%.1fs | avg_tps=%.0f",
        total_sent,
        total_time,
        total_sent / total_time,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Fraud detection event producer")
    parser.add_argument("--tps", type=int, default=500, help="Target events per second")
    parser.add_argument("--duration", type=int, default=300, help="Run duration in seconds")
    parser.add_argument("--anomaly-rate", type=float, default=0.02, help="Fraction of anomalous events")
    args = parser.parse_args()
    asyncio.run(run(args.tps, args.duration, args.anomaly_rate))


if __name__ == "__main__":
    main()
