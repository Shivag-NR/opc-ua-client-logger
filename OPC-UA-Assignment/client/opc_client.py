from opcua import Client
from datetime import datetime
import time
import csv
import os

# ==============================
# CONFIGURATION
# ==============================

SERVER_URL = "opc.tcp://localhost:4841"   # make sure this matches server
READ_INTERVAL_SECONDS = 60               # log every 60 seconds

# ==============================
# CONNECT TO OPC UA SERVER
# ==============================

client = Client(SERVER_URL)
client.connect()
print("Connected to OPC UA Server")

# ==============================
# GET TAGS USING BROWSING
# ==============================

objects = client.get_objects_node()
dummy_obj = objects.get_child(["2:DummyData"])

nodes = []
for i in range(1, 11):
    nodes.append(dummy_obj.get_child([f"2:Tag{i}"]))

# ==============================
# PREPARE OUTPUT DIRECTORY
# ==============================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

current_hour = None
csv_file = None
csv_writer = None

# ==============================
# MAIN LOGGING LOOP
# ==============================

try:
    while True:
        now = datetime.now()
        hour_str = now.strftime("%Y-%m-%d_%H")

        # Create new CSV file when hour changes
        if current_hour != hour_str:
            if csv_file:
                csv_file.flush()
                os.fsync(csv_file.fileno())
                csv_file.close()

            filename = os.path.join(
                OUTPUT_DIR, f"OPC_Log_{hour_str}.csv"
            )
            csv_file = open(filename, "a", newline="")
            csv_writer = csv.writer(csv_file)

            # Write header
            csv_writer.writerow([
                "Timestamp",
                "EpochTime",
                "Tag1", "Tag2", "Tag3", "Tag4", "Tag5",
                "Tag6", "Tag7", "Tag8", "Tag9", "Tag10"
            ])

            csv_file.flush()
            os.fsync(csv_file.fileno())
            current_hour = hour_str

        # Read OPC UA tag values
        values = [node.get_value() for node in nodes]

        row = [
            now.strftime("%Y-%m-%d %H:%M:%S"),
            int(now.timestamp()),
            *values
        ]

        # Write row + FORCE DISK FLUSH
        csv_writer.writerow(row)
        csv_file.flush()
        os.fsync(csv_file.fileno())

        print("Row written to CSV:", row)

        time.sleep(READ_INTERVAL_SECONDS)

except KeyboardInterrupt:
    print("Client stopped by user")

finally:
    if csv_file:
        csv_file.flush()
        os.fsync(csv_file.fileno())
        csv_file.close()
    client.disconnect()
    print("Client disconnected cleanly")
