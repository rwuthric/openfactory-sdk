import time
import csv
from openfactory.assets import Asset
from openfactory.kafka import KSQLDBClient

ksql = KSQLDBClient('http://localhost:8088')
ivac = Asset('IVAC', ksqlClient=ksql)


def on_event(msg_key, msg_value):
        with open('ivac_events.csv', 'a', newline='') as csvfile:
            fieldnames = []
            for key in msg_value.keys():
                fieldnames.append(key)

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            csvfile.seek(0, 2)
            if csvfile.tell() == 0:
                    writer.writeheader()

            writer.writerow(msg_value)
        print(f"[Event] [{msg_key}] {msg_value}")


ivac.subscribe_to_events(on_event, 'ivac_events_group')

# run a main loop while subscriptions remain active
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping consumer threads ...")
    ivac.stop_events_subscription()
    print("Consumers stopped")
finally:
    ksql.close()
    

