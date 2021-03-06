from confluent_kafka.admin import NewTopic
import time

class KafkaUtils:

    def __init__(self, kafka_client):
        self.kafka_client = kafka_client

    def topic_exists(self, topic_name):
        """
        Checks if the given topic exists
        """

        topic_info = self.kafka_client.list_topics(timeout=5)
        
        return topic_name in set(t.topic for t in iter(topic_info.topics.values()))

    def create_topic(self, topic_name, num_partitions, replication_factor, config_dict):
        """
        Creates a new topic
        """

        futures = self.kafka_client.create_topics(
            [
                NewTopic(
                    topic = topic_name,
                    num_partitions = num_partitions,
                    replication_factor = replication_factor,
                    config = config_dict
                )
            ]
        )

        for topic, future in futures.items():
            try:
                future.result()
                print("Topic '{}' created.".format(topic))
            except Exception as e:
                print("Failed to create the topic '{topic_name}': {error}".format(topic_name=topic, error=e))

    def delete_topic(self, topic_name):
        """
        Delete a topic
        """

        futures = self.kafka_client.delete_topics(
            [topic_name]
        )

        for topic, future in futures.items():
            try:
                future.result()
                print("Topic '{}' deleted.".format(topic))
            except Exception as e:
                print("Failed to delete the topic '{topic_name}': {error}".format(topic_name=topic, error=e))

        time.sleep(30)