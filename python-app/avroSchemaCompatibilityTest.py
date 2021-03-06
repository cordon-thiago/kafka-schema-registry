from confluent_kafka import avro
from modules.dataGenerator import DataGenerator
from modules.kafkaUtils import KafkaUtils
from modules.schemaRegistryUtils import SchemaRegistryUtils
from confluent_kafka.admin import AdminClient
from inspect import getmembers, isfunction
import json
import time
import argparse

#################################################
# Define and parse args
#################################################
parser = argparse.ArgumentParser()

parser.add_argument(
    "--compatibility_type_list", #param name
    nargs="*", #number of items in list
    type=str, #type
    required=True,
    choices=["BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE", "NONE"] 
)

parser.add_argument(
    "--topic_name",
    type=str,
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
# Generate schemas
#################################################
# Get schema names (The name of functions from the module DataGenerator)
schema_function_lst = [func_name for func_name, _ in getmembers(DataGenerator, isfunction) if func_name not in ["__init__", "avro_schema"]] 

# Instantiate DataGenerator
data_gen = DataGenerator()

# Generate dict with schemas to check. Pattern: {"name_of_schema": schema(dict string)}
schemas_to_check_dict = {}
for schema_function in schema_function_lst:
    schema = {"schema":json.dumps(data_gen.avro_schema(schema_function)["schema"])}
    schemas_to_check_dict[schema_function] = schema

#################################################
# Test schema compatibility
#################################################

# Instantiate SchemaRegistryUtils
sr_utils = SchemaRegistryUtils(schema_registry_url)

# Test schema compatibility for each compatibility level defined in args.compatibility_type_list
for schema_compatibility in args.compatibility_type_list:

    print("\n")
    print("###################################")
    print("Running '{}' schema check.".format(schema_compatibility))
    print("###################################")

    print(json.dumps(
            sr_utils.check_schema_compatibility(schemas_to_check_dict, subject_name, schema_compatibility),
            indent=4
        ) 
    )

