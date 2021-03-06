# References:
## Example of schema validation: https://github.com/confluentinc/schema-registry/blob/master/core/src/test/java/io/confluent/kafka/schemaregistry/avro/AvroCompatibilityTest.java
## Avro producer: https://github.com/confluentinc/confluent-kafka-python
## Test compatibility: https://docs.confluent.io/platform/current/schema-registry/develop/using.html#test-compatibility-of-a-schema-with-the-latest-schema-under-subject-kafka-value

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from modules.dataGenerator import DataGenerator
from confluent_kafka.admin import AdminClient
from modules.kafkaUtils import KafkaUtils
from inspect import getmembers, isfunction
import argparse
import json

#################################################
# Define and parse args
#################################################
parser = argparse.ArgumentParser()

parser.add_argument(
    "--schema_name", #param name
    nargs="*", #number of items in list
    type=str, #type
    required=True,
    choices=[func_name for func_name, _ in getmembers(DataGenerator, isfunction) if func_name not in ["__init__", "avro_schema"]] 
)

parser.add_argument(
    "--topic_name",
    type=str,
    required=True
)

parser.add_argument(
    "--qty_messages",
    type=int,
    required=True
)

args = parser.parse_args()

#################################################
# Variables
#################################################
bootstrap_servers = "broker:29092"
topic_name = args.topic_name
topic_config = {
    "cleanup.policy": "delete",
    "delete.retention.ms": "86400000",
    "file.delete.delay.ms": "86400000",
    "confluent.value.schema.validation": True
}
qty_messages = args.qty_messages

# Instantiate Kafka client admin
kafka_client = AdminClient(
    {"bootstrap.servers": bootstrap_servers}
)

#################################################
# Create topic
#################################################
# Instantiate KafkaUtils
kafka_utils = KafkaUtils(kafka_client)

if not kafka_utils.topic_exists(topic_name):
    kafka_utils.create_topic(topic_name, 1, 1, topic_config)
else:
    print("Topic '{}' already exists.".format(topic_name))


#################################################
# Generate data
#################################################
def delivery_report(err, msg):

    if err is not None:
        print("Message delivery failed: {fail}".format(fail=err))
    else:
        print("Message delivered to {topic} [{partition}]".format(topic=msg.topic(), partition=msg.partition()))

data_gen = DataGenerator()

for schema_name in args.schema_name:

    print("###################################")
    print("Generating messages for schema '{schema_name}'.".format(schema_name=schema_name))
    print("###################################")

    # Configure kafka broker and schema registry
    avroProducer = AvroProducer({
        'bootstrap.servers': 'broker:29092',
        'on_delivery': delivery_report,
        'schema.registry.url': 'http://schema-registry:8081'
        }, default_value_schema=avro.loads(json.dumps(data_gen.avro_schema(schema_name)["schema"])))

    # Generate messages
    for _ in range(qty_messages):
        avroProducer.produce(topic=topic_name, value=data_gen.avro_schema(schema_name)["value"])
        avroProducer.flush()
 


