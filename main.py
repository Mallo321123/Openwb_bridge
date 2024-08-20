import configparser
import paho.mqtt.client as mqtt
import threading
import json

topics_config = configparser.ConfigParser()
topics_config.read('topics.conf')

generall_config = configparser.ConfigParser()
generall_config.read('generall.conf')

global charge_state
charge_State = None

def convert(value, name):
    global charge_state
    if name == "charge_state":      # charge state übersetzen
        if value == "false":
            charge_state = 0
        else:
            charge_state = 1
            
    if name == "charge_mode":       # Charge mode übersetzen
        charge_modes = {
            '"standby"': 0,
            '"stop"': 1,
            '"scheduled_charging"': 2,
            '"instant_charging"': 3,
            '"pv_charging"': 4
        }
        cleaned_value = str(value.strip().lower())
        
        if cleaned_value in charge_modes:
            return charge_modes[cleaned_value]
        elif len(cleaned_value) == 0:
            return ValueError
        else:
            print("Fehler bei charge_mode")
            return 10
    
    if name == "charge_phases" and charge_state == 0:    # Charge phases übersetzen
        return(0)
        
    value = value.strip().lower()
    
    if value == "false":
        return 0
    elif value == "true":
        return 1

    try:
        # Versuche, den Wert zu einem Integer zu konvertieren
        int_value = float(value)
        return int_value
    except ValueError:
        try:
            # Versuche, den Wert als JSON-Array zu parsen
            list_value = json.loads(value)
            if isinstance(list_value, list):
                # Konvertiere jedes Element der Liste zu Float
                return [float(x) for x in list_value]
            else:
                return value
        except (ValueError, json.JSONDecodeError):
            return value

def openwb_on_message(client, userdata, msg):
    src_topic = msg.topic
    payload = msg.payload.decode('utf-8')
    section_name = userdata.get(src_topic, {}).get('section')
    dest_topic = userdata.get(src_topic, {}).get('dest_topic')
    
    if section_name and dest_topic:
        if payload == "":
            return
        data_value = convert(payload, section_name)
        
        data = {
            section_name: data_value
        }   
        json_payload = json.dumps(data)
        
        #print(f"Weiterleiten von {src_topic} zu {dest_topic}: {json_payload}")
        mosqitto_client.publish(dest_topic, json_payload)
    else:
        print(f"Kein Ziel-Topic oder Abschnitt für {src_topic} gefunden")


mosqitto_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
openwb_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

userdata = {}
for section in topics_config.sections():
    src_topic = topics_config[section]['src_topic']
    dest_topic = topics_config[section]['dest_topic']
    userdata[src_topic] = {
        'section': section,
        'dest_topic': dest_topic
    }
    
    
openwb_client.user_data_set(userdata)
openwb_client.on_message = openwb_on_message
    
def mosqitto_mqtt():
    mosqitto_client.connect(generall_config['mqtt_mosquitto']['ip'], int(generall_config['mqtt_mosquitto']['port']), 60)
    print("mosquitto MQTT connected")
    mosqitto_client.loop_forever()
    
def openwb_mqtt():
    openwb_client.connect(generall_config['mqtt_wallbox']['ip'], int(generall_config['mqtt_wallbox']['port']), 60)
    print("openwb MQTT connected")
    
    for src_topic in userdata.keys():
        result = openwb_client.subscribe(src_topic)
        if result[0] == mqtt.MQTT_ERR_SUCCESS:
            print(f"Erfolgreich abonniert: {src_topic}")
        else:
            print(f"Fehler beim Abonnieren von {src_topic}: {result}")
            
    openwb_client.loop_forever()
    
mosqitto_mqtt_thread = threading.Thread(target=mosqitto_mqtt)
mosqitto_mqtt_thread.start()

openwb_mqtt_thread = threading.Thread(target=openwb_mqtt)
openwb_mqtt_thread.start()