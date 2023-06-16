# imports:
from uuid import uuid4
from logger_dir import LOG_FILE
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
import logging
from generic import Generic, instance_to_dict
from kafka_config import schema_config, sasl_conf


# function call for all generic function : and delivery report

def delivery_report(err, msg):
    if err is not None:
        logging.info("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    logging.info('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def product_data_using_file(topic, file_path):
    logging.info(f"Topic : {topic} FilePath : {file_path}")
    schema_str = Generic.schema_structure(file_path=file_path)
    schema_registry_client = SchemaRegistryClient(schema_config())

    json_serializer = JSONSerializer(schema_str, schema_registry_client, instance_to_dict)
    print("Producing user records to topic {}. ^C to exit.".format(topic))
    producer = Producer(sasl_conf())
    producer.poll(0)
    try:
        for i in Generic.get_object(file_path=file_path):
            print(i)
            logging.info(f"Topic: {topic} file_path:{i.to_dict()}")

            producer.produce(topic=topic,
                             key=StringSerializer('utf_8')((str(uuid4())), i.to_dict()),
                             value=json_serializer(i, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)

            print("\nFlushing records...")
            producer.flush()
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass


topic = 'kafka_Sensors'
file_path = 'kafka-Sensors/Aps_failure_training_set1.csv'
product_data_using_file(topic, file_path)
