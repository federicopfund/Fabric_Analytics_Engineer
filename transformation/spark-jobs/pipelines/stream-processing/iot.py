#!/usr/bin/python3

# imports

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from time import time, sleep
from sys import argv, exit
import numpy as np             
import threading

# pip install kafka-python
# pip install numpy

# different device "profiles" with different 
# distributions of values to make things interesting
# tuple --> (mean, std.dev)

	
DEVICE_PROFILES = {
	"SanRafael": {'temp': (51.3, 17.7), 'humd': (77.4, 18.7), 'pres': (1019.9, 9.5) },
	"25DeMayo": {'temp': (49.5, 19.3), 'humd': (33.0, 13.9), 'pres': (1012.0, 41.3) },
	"Neuquen": {'temp': (63.9, 11.7), 'humd': (62.8, 21.8), 'pres': (1015.9, 11.3) },
}

class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self,profile_name,profile):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        while not self.stop_event.is_set():
			
			temp = np.random.normal(profile['temp'][0], profile['temp'][1])
			humd = max(0, min(np.random.normal(profile['humd'][0], profile['humd'][1]), 100))
			pres = np.random.normal(profile['pres'][0], profile['pres'][1])
			
			# create CSV structure
			msg = f'{time()},{profile_name},{temp},{humd},{pres}'

			# send to Kafka

            producer.send('divice', b"Test")
            producer.send('divice', b"\xc2Conectando con divice!")
			producer.send('weather', bytes(msg, encoding='utf8'))
			print(f'sending data to kafka, #{count}')

			count += 1
            time.sleep(1)
        producer.close()


class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(['weather'])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                if self.stop_event.is_set():
                    break

        consumer.close()

def main(devices):
	try:
		# check for arguments, exit if wrong
		if len(argv) != 2 or argv[1] not in devices.keys():
			print("please provide a valid device name:")
			for key in devices.keys():
				print(f"  * {key}")
			print(f"\nformat: {argv[0]} DEVICE_NAME")
			exit(1)
		
		profile_name = argv[1]
		profile = devices[profile_name]
		
        admin = KafkaAdminClient(bootstrap_servers='localhost:9092')

        topic = NewTopic(name='weather',
                         num_partitions=1,
                         replication_factor=1)

        admin.create_topics([topic])
    except Exception:
        pass

    tasks = [
        Producer(),
        Consumer()
    ]

    # Start threads of a publisher/producer and a subscriber/consumer to 'my-topic' Kafka topic
    for t in tasks:
        t.start()

    time.sleep(10)

    # Stop threads
    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == "__main__":
    main(DEVICE_PROFILES)