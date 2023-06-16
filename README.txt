1. Setup environment
2. run setup.py from tools
3. setup the kafka configuration in .env file and create call for env---as kafka_cofig.py
4. create generic file for for providing the schema structure and json_record_generator --- generic.py
5.  create producer_main calling functions (ie. topic name,schema, serializer)to send data to kafka