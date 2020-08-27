import re
from typing import NamedTuple

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

INFLUXDB_ADDRESS = '192.168.2.113'
INFLUXDB_USER = 'Hortiya'
INFLUXDB_PASSWORD = 'Hortiya123'
INFLUXDB_DATABASE = 'Plant_station'

MQTT_ADDRESS = '192.168.2.113'
MQTT_USER = 'Hortiya'
MQTT_PASSWORD = 'Hortiya123'
MQTT_TOPIC1 = 'Thermocouple'
MQTT_TOPIC2 = 'SoilSensor'
MQTT_CLIENT_ID = 'Thermo'

influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, 8086, INFLUXDB_USER, INFLUXDB_PASSWORD, None)


class SensorData(NamedTuple):
    Sensor: str
    measurement: str
    value: float


def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC1)
    client.subscribe(MQTT_TOPIC2)

def _parse_mqtt_message(topic, payload):
    #match = re.match(MQTT_REGEX, topic)
    match = topic
    if (match=='Thermocouple'):
        Sensor = topic
        measurement = topic
        if measurement == 'status':
            return None
      
        return SensorData(Sensor, measurement, float(payload))
    
    elif (topic == 'Temperature'):
        Sensor = match
        measurement = 'STemperature'
        print(payload.split(",")[0])
        payload = float(payload.split(",")[0])
        #SensorData(Sensor, measurement, payload)
        #_send_sensor_data_to_influxdb(SensorData)
        #measurement = 'SVWC'
        #payload = float(payload.split(",")[1])
        #SensorData(Sensor, measurement, payload)
        #_send_sensor_data_to_influxdb(SensorData)
        #measurement = 'EC'
        #payload = float(payload.split(",")[2])
        
        return SensorData(Sensor, measurement, payload)
    
    elif (topic == 'VWC'):
        Sensor = match
        measurement = 'VWC'
        print(payload.split(",")[1])
        payload = float(payload.split(",")[1])
        return SensorData(Sensor, measurement, payload)
    
    
    elif (topic == 'EC'):
        Sensor = match
        measurement = 'EC'
        print(payload.split(",")[2])
        payload = float(payload.split(",")[2])
        return SensorData(Sensor, measurement, payload)

    
    else:
        return None



def _send_sensor_data_to_influxdb(sensor_data):
    json_body = [
        {
            'measurement': sensor_data.measurement,
            'tags': {
                'Sensor': sensor_data.Sensor
            },
            'fields': {
                'value': sensor_data.value
            }
        }
    ]
    influxdb_client.write_points(json_body)
    print(sensor_data)


def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    print(msg.topic + ' ' + (msg.payload.decode('utf-8')))
    sensor_data = _parse_mqtt_message(msg.topic, msg.payload.decode('utf-8'))
    if sensor_data is not None:
        _send_sensor_data_to_influxdb(sensor_data)
    if (msg.topic == 'SoilSensor'):
        sensor_data = _parse_mqtt_message('Temperature',msg.payload.decode('utf-8'))
        _send_sensor_data_to_influxdb(sensor_data)
        sensor_data = _parse_mqtt_message('VWC',msg.payload.decode('utf-8'))
        _send_sensor_data_to_influxdb(sensor_data)
        sensor_data = _parse_mqtt_message('EC',msg.payload.decode('utf-8'))
        _send_sensor_data_to_influxdb(sensor_data)

        
def _init_influxdb_database():
    databases = influxdb_client.get_list_database()
    if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
        influxdb_client.create_database(INFLUXDB_DATABASE)
    influxdb_client.switch_database(INFLUXDB_DATABASE)

def main():
    _init_influxdb_database()

    mqtt_client = mqtt.Client()
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_ADDRESS, 1883)
    mqtt_client.loop_forever()


if __name__ == '__main__':
    print('MQTT to InfluxDB bridge')
    main()
