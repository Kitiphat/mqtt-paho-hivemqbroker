# mqtt-paho-hivemqbroker
 
# Project I

> This project is about sending and receiving data from using MQTT protocol. This project is part of subject CPE314 Network Systems from the **7 โมงเช้า** group. This project uses Python 3.10 to develop and test.

## Project Info
Broker: HiveMQ MQTT Broker
Language: Python
Database: MySQL ( phpMyAdmin )

## Explanation about MQTT
MQTT is the most commonly used messaging protocol for the Internet of Things (IoT). MQTT stands for MQ Telemetry Transport. The protocol is a set of rules that defines how IoT devices can publish and subscribe to data over the Internet. MQTT is used for messaging and data exchange between IoT and industrial IoT (IIoT) devices. The protocol is event driven and connects devices using the publish /subscribe (Pub/Sub) pattern. The sender (Publisher) and the receiver (Subscriber) communicate via Topics and are decoupled from each other. The connection between them is handled by the MQTT broker. The MQTT broker filters all nodes incoming messages and distributes them correctly to the Subscribers to that topic

```
* python --version
Python 3.10.10
```

## Using pip to install the Paho MQTT client
pip is the package installer for Python. You can use pip to install packages from the Python Package Index and other indexes.

```
pip install paho-mqtt
```

## Installing pandas

pandas is a fast, powerful, flexible and easy to use open source data analysis and manipulation tool,
built on top of the Python programming language.
```
pip install pandas
```

## Installing MySQL Connector

MySQL Connectors provide connectivity to the MySQL server for client programs.
```
pip install mysql-connector-python
``` 
## === File Description ===
 
publish.py -> This is a file that read the datas from Excel and send them to Broker then the datas will go to the server at the end.

sub.py -> This is a file that receive datas from Broker to query and keep them in a database.

SampleInput.xlsx -> This is the datas of client which is sub.py will send them in to the database.

input_sensor.sql -> This is a SQL file that we create the database for this project.
