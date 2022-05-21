from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1, storage

#https://cloud.google.com/pubsub/docs/pull

# TODO(developer)
project_id = "eastern-academy-xxxxxx"
subscription_id = "topic_xxxx-sub"
# Number of seconds the subscriber should listen for messages
timeout = 3600.0

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

buy, book, search = [], [], []

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {message.data!r}.")
        if message.attributes:
            print("Attributes:")
            for key in message.attributes:
                value = message.attributes.get(key)
                if value == 'buy':
                    buy.append(str(message.data)+'\n')
                elif value == 'book':
                    book.append(str(message.data)+'\n')
                else:
                    search.append(str(message.data)+'\n')
                print(f"{key}: {value}")
        message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        result = streaming_pull_future.result(timeout=timeout) # timeout=timeout
    except TimeoutError:
        subscriber.close()
        #streaming_pull_future.cancel()  # Trigger the shutdown.

# open the file in the write mode
f = open('buy.csv', 'a')
f.writelines(buy)
f.close()

g = open('book.csv', 'a')
g.writelines(book)
g.close()

h = open('search.csv', 'a')
h.writelines(search)
h.close()


storage_client = storage.Client()

bucket_name = "example-xxxx"
bucket = storage_client.bucket(bucket_name)

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

upload_blob("example-xxxx", "/tmp/buy.csv", "buy.csv")
upload_blob("example-xxxx", "/tmp/book.csv", "book.csv")
upload_blob("example-xxxx", "/tmp/search.csv", "search.csv")