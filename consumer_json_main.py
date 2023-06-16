# imports


# consumer function:
from confluent_kafka import Consumer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from generic import Generic
from kafka_config import sasl_conf
from logger_dir.M_DB_looger_file import Mongo_Operations


def consumer_using_file(topic, file_path):
    schema_str = Generic.schema_structure(file_path)
    json_deserializer = JSONDeserializer(schema_str, Generic.dict_to_object)

    consumer_conf = sasl_conf()
    consumer_conf.update({
        'group.id': 'group_1',
        'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    mongodb = Mongo_Operations()

    records = []
    x = 0
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            record: Generic = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if record is not None:
                records.append(record.to_dict())
                if x % 5000 == 0:
                    mongodb.Insert_Multiple_Record(DATABASE_NAME="DATABASE_APS_FAULT", COLLECTION_NAME="Aps_Fault",
                                                   MULTIPLE_RECORD=records)
                    records = []
            x = x + 1
        except KeyboardInterrupt:
            break

    consumer.close()


topic = 'kafka_Sensors'
file_path = 'kafka-Sensors/Aps_failure_training_set1.csv'
consumer_using_file(topic, file_path)