import os
import time

import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

token = os.environ.get("INFLUXDB_TOKEN")

org = "jerryhong"
url = "http://localhost:8086"

write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)


bucket = "jerrymmm"

write_api = write_client.write_api(write_options=SYNCHRONOUS)

for value in range(5):
    point = Point("measurement1").tag("tagname1", "tagvalue1").field("field1", value)
    write_api.write(bucket=bucket, org="jerryhong", record=point)
    time.sleep(1)  # separate points by 1 second

query_api = write_client.query_api()

query = """from(bucket: "jerrymmm")
 |> range(start: -60m)
 |> filter(fn: (r) => r._measurement == "measurement1")"""
tables = query_api.query(query, org="jerryhong")

for table in tables:
    for record in table.records:
        print(record)


query = """from(bucket: "jerrymmm")
  |> range(start: -30m)
  |> filter(fn: (r) => r._measurement == "measurement1")
  |> mean()"""
tables = query_api.query(query, org="jerryhong")

for table in tables:
    for record in table.records:
        print(record)
