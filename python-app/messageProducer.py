from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from modules.dataGenerator import DataGenerator
from confluent_kafka.admin import AdminClient
from modules.kafkaUtils import KafkaUtils
from modules.schemaRegistryUtils import SchemaRegistryUtils
from inspect import getmembers, isfunction
import argparse
import json
import time

#################################################
# Define and parse args
#################################################
parser = argparse.ArgumentParser()

parser.add_argument(
    "--compatibility_type",
    type=str,
    required=True,
    choices=["BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE", "NONE"] 
)

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
schema_registry_url = "http://schema-registry:8081"
subject_name = "{topic_name}-value".format(topic_name = topic_name)
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

# Delete topic if it exists (try 3 times if it is not deleted for the first time)
num_tries = 0
while kafka_utils.topic_exists(topic_name) and num_tries <= 3:
    kafka_utils.delete_topic(topic_name)
    num_tries = num_tries + 1

print("Number of tries to delete the topic: {}".format(num_tries))

# Create topic
kafka_utils.create_topic(topic_name, 1, 1, topic_config)

#################################################
# Generate data
#################################################
def delivery_report(err, msg):

    if err is not None:
        print("Message delivery failed: {fail}".format(fail=err))
    else:
        print("Message delivered to {topic} [{partition}]".format(topic=msg.topic(), partition=msg.partition()))

data_gen = DataGenerator()
sr_utils = SchemaRegistryUtils(schema_registry_url)

for schema_num, schema_name in enumerate(args.schema_name):

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
    for msg_num in range(qty_messages):
        avroProducer.produce(topic=topic_name, value=data_gen.avro_schema(schema_name)["value"])
        avroProducer.flush()
        if schema_num == 0 and msg_num == 0:
            # Set schema compatibility
            sr_utils.set_subject_compatibility(subject_name, args.compatibility_type)
            

        
 


