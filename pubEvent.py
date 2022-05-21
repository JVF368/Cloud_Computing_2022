import time
from google.cloud import pubsub_v1

project_id = "eastern-academy-xxxxxx"
topic_id = "topic_xxxx" # A single topic for all 3 kinds of messages

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

f = open("events.csv","r")
while True:
    line = f.readline()
    if not line:
        break
    
    sleepingTime,topic,message = line.split(",")
    
    sleepingTime = int(sleepingTime)
    message = message.replace("\n","")
    
    print("Publishing a topic: '%s' with message: %s"%(topic,message))

	# TODO PUB 
	# https://cloud.google.com/pubsub/docs/publisher#python
    message = message.encode("utf-8")
    topic = topic.encode("utf-8")
    future = publisher.publish(topic_path, message, topic_ = topic) # topic=topic topic is the only attribute I need when publishing it.
    print(future.result())
    
    print("Waiting %i seconds"%sleepingTime)
    time.sleep(sleepingTime)

print("Done")

