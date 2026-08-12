import csv
import sys
import time
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "browser-history"
DELAY = 0.01


def delivery_callback(err, msg):
    if err:
        print(f"[ERROR] Cannot send message: {err}")


def extract_url_from_row(row: dict) -> str | None:
    for key in ["url", "URL", "Url"]:
        if key in row and row[key].strip():
            return row[key].strip()
    return None


def main(csv_path: str):
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    print(f"=== Generator running ===")
    print(f"Reading: {csv_path}")
    print(f"Sending to topic: {TOPIC}\n")

    sent = 0
    skipped = 0

    with open(csv_path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)

        for row in reader:
            url = extract_url_from_row(row)

            if not url:
                skipped += 1
                continue

            # Sending URL as message value
            producer.produce(
                topic=TOPIC,
                value=url.encode("utf-8"),
                callback=delivery_callback,
            )
            sent += 1

            # Every 100 messages — flush and report
            if sent % 100 == 0:
                producer.poll(0)
                print(f"Sent: {sent} URLs...")

            time.sleep(DELAY)

    producer.flush()
    print(f"\n=== Done ===")
    print(f"Sent: {sent} messages")
    print(f"Skipped (no URL): {skipped} rows")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generator.py <path_to_history.csv>")
        print("Example: python generator.py ~/Downloads/BrowserHistory.csv")
        sys.exit(1)

    main(sys.argv[1])