import pandas as pd
import json


class Generic:
    def __init__(self, record: dict):
        for ky, val in record.items():
            setattr(self, ky, val)

    def to_dict(self):
        return self.__dict__

    @staticmethod
    def dict_to_object(data: dict,ctx):
        print(data)
        return Generic(record=data)



    @classmethod
    def get_object(cls, file_path):
        chunk_df = pd.read_csv(file_path, chunksize=10)
        n = 0
        for data in chunk_df:
            for df in data.values:
                generic = Generic(dict(zip(data.columns, list(map(str, df)))))
                n += 1
                yield generic

    # defining the schema for confluent--> Json format

    @classmethod
    def schema_structure(cls, file_path):
        columns = next(pd.read_csv(file_path, chunksize=10)).columns
        schema = dict()
        schema.update({
            "title": "Aps_Data",
            "namespace": "com.mycorp.mynamespace",
            "name": "Sensor_fault",
            "doc": "Sample schema to get started.",
            "fields": dict()
        })
        for column in columns:
            schema["fields"].update(
                {f"{column}": {
                    "type": f"{column}"
                }
                })
        schema = json.dumps(schema)
        print(schema)
        return schema

    """
    @staticmethod
    def __str__(self):
        return f"{self.__dict__}"  # Generic.__str__(Generic(aa))
    """


def instance_to_dict(instance: Generic, ctx):
    return instance.to_dict()
