from json import dumps
from os import environ
import requests
import time
import json
import datetime
from datetime import datetime, timedelta
import pytz


from kafka import KafkaProducer

MAX_ENTRIES = 50
KAFKA_TOPIC = environ.get("KAFKA_TOPIC", "earthquakes-topic")

KAFKA_CONFIGURATION = {
    "bootstrap_servers": environ.get("KAFKA_BROKER", "kafka1:19092").split(","),
    "key_serializer": lambda x: str.encode("" if not x else x, encoding='utf-8'),
    "value_serializer": lambda x: dumps(dict() if not x else x).encode(encoding='utf-8'),
    "reconnect_backoff_ms": int(100)
}

def send_to_kafka(data):
    try:
        connecting = True
        entries = 0
      
        while connecting and entries<MAX_ENTRIES:
            try:
                print("Configuration")
                print(KAFKA_CONFIGURATION)
                producer = KafkaProducer(**KAFKA_CONFIGURATION)
                if producer.bootstrap_connected():
                    connecting = False
            except Exception as e:
                entries +=1
                print(f"Kafka-producer connection error: {e}")
            print(f"Connecting to Kafka ({entries})...")

        if entries>=MAX_ENTRIES:
            print("Cannot connect to Kafka.")
            return

        print(f"Kafka successfullly connected. Connected to bootsrap servers.")
        
        try:
            json_object = json.loads(data)
            print("The dictionary is a valid JSON object.")
        except ValueError:
            print("The dictionary is not a valid JSON object.")

        results = json_object.get("results", None)
        for result in results:
            message = result
            producer.send(topic=KAFKA_TOPIC, key=json_object.get("id", None), value=message)
    
        producer.flush() 
        
    except Exception as e:
        print(f"Error: {e}.")
        

def main():
    time.sleep(30)   
    # half_minute_ago_formatted = "2018-01-21T00:00:00"
    # current_time_formatted = "2018-01-21T00:15:00"
    
    current_time = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(days=(6*365))

    while True:
        try:
            current_time = current_time + timedelta(minutes=15)
            half_minute_ago = current_time - timedelta(minutes=15)

            current_time_formatted = current_time.strftime("%Y-%m-%dT%H:%M:%S")
            half_minute_ago_formatted = half_minute_ago.strftime("%Y-%m-%dT%H:%M:%S")

            earthquake_url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={0}&endtime={1}".format(half_minute_ago_formatted, current_time_formatted) 
            print(earthquake_url)
            response = requests.get(url=earthquake_url)

            if response.ok:
                data = response.json()
                print(data)

                all = []
                for i in range(len(data['features'])):
                    curr_earthquake = data['features'][i]

                    curr_earthquake_json = {
                        'longitude': curr_earthquake['geometry']['coordinates'][0],
                        'latitude': curr_earthquake['geometry']['coordinates'][1],
                        'depth': curr_earthquake['geometry']['coordinates'][2],
                        'location': curr_earthquake['properties']['place'],
                        'magnitude': curr_earthquake['properties']['mag'],
                        'time': curr_earthquake['properties']['time'],
                        'tsunami': curr_earthquake['properties']['tsunami'],
                        'status': curr_earthquake['properties']['status']
                    }
                    all.append(curr_earthquake_json)

                data_json = json.dumps({'results':all})
                print("Sending data to kafka ...")
                print(data_json)
                send_to_kafka(data_json)
                time.sleep(30)   
            else:
                raise Exception((f"Response status code: {response.status_code}\nResponse text: {response.text}"))
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
        except Exception as e:
            print(f"OTHER ERROR:{e}")
        


if __name__ == "__main__":
    main()