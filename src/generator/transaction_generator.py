"""Synthetic credit-card transaction generator.

Produces a realistic mix of legitimate and fraudulent transactions to a Kafka topic.
Fraud archetypes are intentionally detectable so a downstream model has signal:
  - foreign card-not-present
  - unusual amount (8-20x typical)
  - online + odd hour
"""
import logging
import os
import random
import signal
import time
from dataclasses import dataclass

from confluent_kafka import Producer

from schemas import MerchantCategory, Transaction

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC = os.getenv("TRANSACTIONS_TOPIC", "transactions")
TXN_PER_SECOND = float(os.getenv("TXN_PER_SECOND", "5"))
FRAUD_RATE = float(os.getenv("FRAUD_RATE", "0.05"))
N_USERS = int(os.getenv("N_USERS", "1000"))
N_MERCHANTS = int(os.getenv("N_MERCHANTS", "200"))


@dataclass
class User:
    user_id: str
    home_country: str
    typical_amount: float


@dataclass
class Merchant:
    merchant_id: str
    category: MerchantCategory
    country: str


def build_users(n: int) -> list[User]:
    countries = ["US", "GB", "IN", "DE", "FR"]
    weights = [60, 15, 10, 10, 5]
    return [
        User(
            user_id=f"u_{i:06d}",
            home_country=random.choices(countries, weights=weights)[0],
            typical_amount=round(random.lognormvariate(3.5, 0.7), 2),
        )
        for i in range(n)
    ]


def build_merchants(n: int) -> list[Merchant]:
    countries = ["US", "GB", "IN", "DE", "FR"]
    weights = [60, 15, 10, 10, 5]
    return [
        Merchant(
            merchant_id=f"m_{i:05d}",
            category=random.choice(list(MerchantCategory)),
            country=random.choices(countries, weights=weights)[0],
        )
        for i in range(n)
    ]


def normal_transaction(user: User, merchants: list[Merchant]) -> Transaction:
    domestic = [m for m in merchants if m.country == user.home_country]
    merchant = random.choice(domestic or merchants)
    amount = max(1.0, round(random.gauss(user.typical_amount, user.typical_amount * 0.3), 2))
    return Transaction(
        user_id=user.user_id,
        merchant_id=merchant.merchant_id,
        merchant_category=merchant.category,
        amount=amount,
        country_code=merchant.country,
        is_card_present=random.random() < 0.7,
        is_fraud=False,
    )


def fraud_transaction(user: User, merchants: list[Merchant]) -> Transaction:
    archetype = random.choice(["foreign", "unusual_amount", "odd_hour_online"])
    if archetype == "foreign":
        foreign = [m for m in merchants if m.country != user.home_country]
        merchant = random.choice(foreign or merchants)
        amount = round(random.gauss(user.typical_amount, user.typical_amount * 0.3), 2)
    elif archetype == "unusual_amount":
        merchant = random.choice(merchants)
        amount = round(user.typical_amount * random.uniform(8, 20), 2)
    else:
        online = [m for m in merchants if m.category == MerchantCategory.ONLINE]
        merchant = random.choice(online or merchants)
        amount = round(random.gauss(user.typical_amount, user.typical_amount * 0.5), 2)

    return Transaction(
        user_id=user.user_id,
        merchant_id=merchant.merchant_id,
        merchant_category=merchant.category,
        amount=max(1.0, amount),
        country_code=merchant.country,
        is_card_present=False,   # CNP is the dominant fraud channel
        is_fraud=True,
    )


def delivery_callback(err, msg):
    if err:
        logger.error("Delivery failed for %s: %s", msg.key(), err)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    users = build_users(N_USERS)
    merchants = build_merchants(N_MERCHANTS)
    logger.info("Pool: %d users, %d merchants", len(users), len(merchants))

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 50,
        "compression.type": "lz4",
        "acks": "all",
        "enable.idempotence": True,
    })

    stop = False

    def shutdown(*_):
        nonlocal stop
        logger.info("Shutting down...")
        stop = True

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    interval = 1.0 / TXN_PER_SECOND
    total = fraud_count = 0

    while not stop:
        user = random.choice(users)
        if random.random() < FRAUD_RATE:
            txn = fraud_transaction(user, merchants)
            fraud_count += 1
        else:
            txn = normal_transaction(user, merchants)
        total += 1

        producer.produce(
            TOPIC,
            key=txn.user_id.encode(),                # same key → same partition → ordering per user
            value=txn.model_dump_json().encode(),
            callback=delivery_callback,
        )
        producer.poll(0)

        if total % 100 == 0:
            logger.info("Produced %d (%.1f%% fraud)", total, 100 * fraud_count / total)

        time.sleep(interval)

    producer.flush(10)
    logger.info("Final: %d produced, %d fraud", total, fraud_count)


if __name__ == "__main__":
    main()