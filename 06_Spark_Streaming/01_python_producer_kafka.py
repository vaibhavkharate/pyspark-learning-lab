# 06_Spark_Streaming/01_python_producer_kafka.py
# in this file, we are creating a simple Flask application that acts as a Kafka producer. The application listens for POST requests at the '/send' endpoint, and when it receives a request, it sends the JSON data to a Kafka topic named 'test-topic'. The Kafka producer is configured to connect to a Kafka broker running at '
# this code snippet demonstrates how to set up a basic Kafka producer using Flask and the kafka-python library. The producer is configured to serialize the data as JSON before sending it to the specified Kafka topic. The application can be tested using a curl command to send a POST request with JSON data.
# kafka-python is a Python client for Apache Kafka, which allows you to produce and consume messages from Kafka topics. In this example, we use the KafkaProducer class to create a producer instance that connects to the specified Kafka broker and sends messages to the 'test-topic' topic. The value_serializer parameter is used to specify how the data should be serialized before sending it to Kafka, in this case, converting the data to JSON format.
# kafka producer is a component that allows you to send messages to Kafka topics. In this example, we create a Kafka producer instance using the KafkaProducer class from the kafka-python library. The producer is configured with the bootstrap_servers parameter, which specifies the address of the Kafka broker to connect to. The value_serializer parameter is used to define how the data should be serialized before sending it to Kafka, in this case, converting the data to JSON format using json.dumps and encoding it as UTF-8.

from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json

app = Flask(__name__)

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers='18.188.225.10:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/send', methods=['POST'])
def send_to_kafka():
    data = request.json
    producer.send('test-topic', data)
    producer.flush()
    return jsonify({"status": "success", "data": data}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

# curl -X POST http://localhost:5000/send -H "Content-Type: application/json" -d '{"key":"My_key", "value":"1,ABC,45"}'