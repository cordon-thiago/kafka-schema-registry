# References:
## Example of schema validation: https://github.com/confluentinc/schema-registry/blob/master/core/src/test/java/io/confluent/kafka/schemaregistry/avro/AvroCompatibilityTest.java
## Avro producer: https://github.com/confluentinc/confluent-kafka-python
## Test compatibility: https://docs.confluent.io/platform/current/schema-registry/develop/using.html#test-compatibility-of-a-schema-with-the-latest-schema-under-subject-kafka-value

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from modules.dataGenerator import DataGenerator
from confluent_kafka.admin import AdminClient
from modules.kafkaUtils import KafkaUtils

#################################################
# Variables
#################################################
bootstrap_servers = "broker:29092"
topic_name = "user_register"
topic_config = {
    "cleanup.policy": "delete",
    "delete.retention.ms": "86400000",
    "file.delete.delay.ms": "86400000",
    "confluent.value.schema.validation": True
}
num_rows = 5

# Instantiate Kafka client admin
kafka_client = AdminClient(
    {"bootstrap.servers": bootstrap_servers}
)

#################################################
# Create topic
#################################################
if not KafkaUtils.topic_exists(kafka_client, topic_name):
    KafkaUtils.create_topic(kafka_client, topic_name, 1, 1, topic_config)
else:
    print("Topic '{}' already exists.".format(topic_name))


#################################################
# Generate data
#################################################
def delivery_report(err, msg):

    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

data_gen = DataGenerator()

# Configure kafka broker and schema registry
avroProducer = AvroProducer({
    'bootstrap.servers': 'broker:29092',
    'on_delivery': delivery_report,
    'schema.registry.url': 'http://schema-registry:8081'
    }, default_value_schema=avro.loads(data_gen.data_schema1()["schema"]))

# Generate messages
for _ in range(num_rows):
    avroProducer.produce(topic='user_register', value=data_gen.data_schema1()["value"])
    avroProducer.flush()
 

