from SingletonStorage import SingletonKeyValueStorage
#################################### basic
ss = SingletonKeyValueStorage()
ss.firestore_backend('project_id','collection')
ss.set('test',{'msg':'yes!'})

#################################### with pub/sub event example
from google.cloud import pubsub_v1
def test_google_pub_sub():
    from google.cloud import pubsub_v1
    publisher = pubsub_v1.PublisherClient()
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_id}`
    class GooglePub:
        def set(self,k,v):
            future = publisher.publish(publisher.topic_path('project_id','topic_id'),
                                f"{k}".encode("utf-8"))
            print(future.result())

    ss = SingletonKeyValueStorage()
    ss.firestore_backend('project_id','collection')
    ss.add_slave(GooglePub())
    ss.set('test',{'msg':'yes!'})

    def subcallback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {message}.")
        message.ack()
    subscriber = pubsub_v1.SubscriberClient()    
    subscriber.subscribe(
        subscriber.subscription_path('project_id', 'topic_id'+'-sub'), callback=subcallback)
    # # Wrap subscriber in a 'with' block to automatically call close() when done.
    # with subscriber:
    #     try:
    #         # When `timeout` is not set, result() will block indefinitely,
    #         # unless an exception is encountered first.
    #         streaming_pull_future.result(timeout=5.0)
    #         print(ss.get('test'))
    #     except TimeoutError:
    #         streaming_pull_future.cancel()  # Trigger the shutdown.
    #         streaming_pull_future.result()  # Block until the shutdown is complete.