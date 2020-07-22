#include <ESP8266WiFi.h>                  // WiFi
#include <EEPROM.h>                       // EEPROM
#include <ArduinoOTA.h>                   // For OTA Programming
#include <PubSubClient.h>                 // MQTT
#include <SensorModbusMaster.h>           // MODBUS
#include <ArduinoJson.h>                  // JSON
#include "secrets.h"                      // User Secrets

#ifdef DEBUG_ESP_PORT
#define DEBUG_MSG(...) DEBUG_ESP_PORT.printf( __VA_ARGS__ )
#else
#define DEBUG_MSG(...)
#endif

////////////////////////////////
// Enums
////////////////////////////////
typedef enum Phases {
  ONE_PHASE,
  THREE_PHASE
};
typedef enum Tariffs {
  THREE_TARIFF,
  SIX_TARIFF
};

////////////////////////////////
// Structs
////////////////////////////////
struct EDPBOX {
  uint8_t Address;
  Phases NumberPhases;
  Tariffs NumberTariffs;
  bool EnergyExport;
  String ThingId;
  unsigned long LAST_INFO_COMMUNICATION = 0;
  unsigned long LAST_TOTAL_COMMUNICATION = 0;
  unsigned long LAST_TARIFF_COMMUNICATION = 0;
  unsigned long LAST_INSTANTANEOUS_COMMUNICATION = 0;

  EDPBOX(uint8_t EDPBOX_Address, Phases EDPBOX_Phases, Tariffs EDPBOX_Tariffs, bool EDPBOX_EnergyExport, String EDPBOX_ThingId) {
    Address = EDPBOX_Address;
    NumberPhases = EDPBOX_Phases;
    NumberTariffs = EDPBOX_Tariffs;
    EnergyExport = EDPBOX_EnergyExport;
    ThingId = EDPBOX_ThingId;
  }

  EDPBOX(uint8_t EDPBOX_Address, String EDPBOX_ThingId) {
    Address = EDPBOX_Address;
    NumberPhases = ONE_PHASE;
    NumberTariffs = THREE_TARIFF;
    EnergyExport = false;
    ThingId = EDPBOX_ThingId;
  }
};

// OTA
#define OTA_HOSTNAME                      "EDPBoxHAN-To-MQTT-1"
#define OTA_PASSWORD                      CONFIG_OTA_PASSWORD

// WiFi
const char* ssid =                        CONFIG_WIFI_SSID;
const char* password =                    CONFIG_WIFI_PASSWORD;

// MQTT
#define MQTT_SERVER                       CONFIG_MQTT_SERVER
#define MQTT_SERVERPORT                   CONFIG_MQTT_SERVERPORT
#define MQTT_USERNAME                     CONFIG_MQTT_USER
#define MQTT_KEY                          CONFIG_MQTT_PASSWORD
#define MQTT_MYNAME                       OTA_HOSTNAME"_Client"

// Modbus
#define MAX485_ENABLE                     5
const int numEDPBoxes =                   2;

////////////////////////////////
// Complex Variables
////////////////////////////////
WiFiClient client;                        // TCP Client
PubSubClient mqttClient(client);          // MQTT
modbusMaster modbus;                      // MODBUS
modbusMaster modbus2;                     // MODBUS

EDPBOX EDPBOXES[numEDPBoxes] = {EDPBOX(0x01, CONFIG_THING_ID_RC), 
                                EDPBOX(0x02, THREE_PHASE, THREE_TARIFF, false, CONFIG_THING_ID_1A)};

////////////////////////////////
// Util functions
////////////////////////////////
#if defined(ESP32) or defined(ESP8266)
#define min(a, b) (((a) < (b)) ? (a) : (b))
#endif

constexpr unsigned int str2int(const char* str, int h = 0) {
    return !str[h] ? 5381 : (str2int(str, h+1) * 33) ^ str[h];
}

String int2weekday(int weekdayInt) {

  String weekdayString;

  switch(weekdayInt) {
    case(1):
      weekdayString = "Monday";
      break;
    case(2):
      weekdayString = "Tuesday";
      break;
    case(3):
      weekdayString = "Wednesday";
      break;
    case(4):
      weekdayString = "Thursday";
      break;
    case(5):
      weekdayString = "Friday";
      break;
    case(6):
      weekdayString = "Saturday";
      break;
    case(7):
      weekdayString = "Sunday";
      break;
  }
  
  return weekdayString;
}

void setClock() {
  configTime(0, 0, "pool.ntp.org", "time.nist.gov");

  time_t now = time(nullptr);
  while (now < 8 * 3600 * 2) {
    delay(500);
    now = time(nullptr);
  }
}

////////////////////////////////
// Time variables
////////////////////////////////
#define REPORTING_INFO_PERIOD             900000
#define REPORTING_TOTAL_PERIOD            150000
#define REPORTING_TARIFF_PERIOD           150000
#define REPORTING_INSTANTANEOUS_PERIOD     30000
#define WATCHDOG_TIMEOUT_PERIOD         86400000

////////////////////////////////
// Variables
////////////////////////////////
char buf[32];

////////////////////////////////
// Setup
////////////////////////////////
void setup() {
  // Init BuiltIn LED
  pinMode(LED_BUILTIN, OUTPUT);
  digitalWrite(LED_BUILTIN, LOW);

  setupSerial();

  setupWiFi();
  setupOTA();
  setupMQTT();

  setupModbus();

  digitalWrite(LED_BUILTIN, HIGH);
}

////////////////////////////////
// Loop
////////////////////////////////
void loop() {
  loopOTA();
  loopMQTT();
  loopModbus();
}

////////////////////////////////
// Modbus
////////////////////////////////
void setupSerial() {
  // Init MODBUS Serial communication
  Serial.begin(9600, SERIAL_8N2);

  // Init DEBUG Serial communication
  Serial1.begin(74880);
}

void setupModbus() {
  // Init Modbus pins
  pinMode(MAX485_ENABLE, OUTPUT);
  
  // Init Modbus2
  //modbus2.begin(0x01, Serial, MAX485_ENABLE);
  //byte message[8] = {0x01, 0x04, 0x00, 0x50, 0x00, 0x0E};
  //int size = modbus2.sendCommand(message, 8);
  //sendBufferMQTT(modbus.responseBuffer, size);
}

void loopModbus() {
  for (int edpbox = 0; edpbox < numEDPBoxes; edpbox++) {
    EDPBOXES[edpbox] = getMeasures(EDPBOXES[edpbox]);
  }
}

////////////////////////////////
// WiFi
////////////////////////////////
void setupWiFi() {
  // Initialize wifi
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);

  const int kRetryCountWiFi = 20;
  int retryCountWiFi = 0;
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    if (retryCountWiFi++ > kRetryCountWiFi) {
      ESP.restart();
    }
  }
}

////////////////////////////////
// MQTT
////////////////////////////////
void setupMQTT() {
  mqttClient.setServer(MQTT_SERVER, MQTT_SERVERPORT);
  mqttClient.setCallback(MQTTOnMessage);

  mqttClient.setBufferSize(2048);
  
  loopMQTT();
}

void loopMQTT() {
  if (!mqttClient.connected()) {
    reconnectMQTT();
  }
  mqttClient.loop();
}

void reconnectMQTT() {
  const int kRetryCountMQTT = 40;
  int retryCountMQTT = 0;
  // Loop until we're reconnected
  while (!mqttClient.connected()) {    
    // Attempt to connect
    if (mqttClient.connect(MQTT_MYNAME, MQTT_USERNAME, MQTT_KEY)) {

      char commandTopicChar[64], debugTopicChar[64];
      String commandTopicString, debugTopicString;

      for (int edpbox = 0; edpbox < numEDPBoxes; edpbox++) {
        commandTopicString = "command/" + EDPBOXES[edpbox].ThingId + "/+";
        commandTopicString.toCharArray(commandTopicChar, 64);
        debugTopicString = "debug/" + EDPBOXES[edpbox].ThingId;
        debugTopicString.toCharArray(debugTopicChar, 64);
        
        mqttClient.subscribe(commandTopicChar);

        mqttClient.publish(debugTopicChar, "PowerMeterHAN Connected!");
      }
    } else {
      // Wait 5 seconds before retrying
      unsigned long now = millis();
      while (millis() - now < 5000) {
        loopOTA();
        yield();
      }
    }
    if (retryCountMQTT++ > kRetryCountMQTT) {
      ESP.restart();
    }
  }
}

void MQTTOnMessage(char* topic, byte* payload, unsigned int length) {
  char commandTopicChar[64], debugTopicChar[64];
  String commandTopicString, debugTopicString;

  for (int edpbox = 0; edpbox < numEDPBoxes; edpbox++) {
    commandTopicString = "command/" + EDPBOXES[edpbox].ThingId + "/restart";
    commandTopicString.toCharArray(commandTopicChar, 64);
    debugTopicString = "debug/" + EDPBOXES[edpbox].ThingId;
    debugTopicString.toCharArray(debugTopicChar, 64);
    
    if (str2int(topic) == str2int(commandTopicChar)) {
      mqttClient.publish(debugTopicChar, "Restarting PowerMeterHAN!");
      ESP.restart();
    }
  }

  String topicString(topic);
  topicString += "/reply";
  char topicChar[64];
  topicString.toCharArray(topicChar, 64);
  mqttClient.publish(topicChar, "Invalid command received!");
}

void sendBufferMQTT(byte* buf, int len) {
  if (mqttClient.beginPublish("debug/"CONFIG_THING_ID_RC, len, false)) {
    uint16_t bytesRemaining = len;
    uint8_t kMaxBytesToWrite = 64;
    uint8_t bytesToWrite;
    uint16_t bytesWritten = 0;
    uint16_t rc;
    bool result = true;

    while((bytesRemaining > 0) && result) {
      bytesToWrite = min(bytesRemaining, kMaxBytesToWrite);
      //bytesToWrite = bytesRemaining;
      rc = mqttClient.write(&(buf[bytesWritten]), bytesToWrite);
      result = (rc == bytesToWrite);
      bytesRemaining -= rc;
      bytesWritten += rc;
    }

    mqttClient.endPublish();
  }
}

void sendBufferMQTT(char buf[], int len) {
  if (mqttClient.beginPublish("debug/"CONFIG_THING_ID_RC, len, false)) {
    for (int i = 0; i < len; i++) {
      mqttClient.write(buf[i]);
    }
    mqttClient.endPublish();
  }
}

////////////////////////////////
// OTA
////////////////////////////////
void setupOTA() {
  // Set OTA Hostname
  ArduinoOTA.setHostname(OTA_HOSTNAME);

  // Set OTA Password
  ArduinoOTA.setPassword((const char *)OTA_PASSWORD);

  // Init OTA
  ArduinoOTA.begin();

  // Loop OTA
  loopOTA();
}

void loopOTA() {
  ArduinoOTA.handle();
}

////////////////////////////////
// EDPBOX
////////////////////////////////
EDPBOX getMeasures(EDPBOX edpbox) {
  // Init Modbus
  modbus.begin(edpbox.Address, Serial, MAX485_ENABLE);

  uint8_t registers;

  size_t capacity = JSON_OBJECT_SIZE(150);
  DynamicJsonDocument doc(capacity);
  char buffer[2048];
  
  char telemetryTopicChar[96], debugTopicChar[64];
  String telemetryTopicString, debugTopicString;
  
  debugTopicString = "debug/" + edpbox.ThingId;
  debugTopicString.toCharArray(debugTopicChar, 64);

  if (millis() - edpbox.LAST_INFO_COMMUNICATION > REPORTING_INFO_PERIOD) {

    digitalWrite(LED_BUILTIN, LOW);

    mqttClient.publish(debugTopicChar, "Reading General Information");
  
    doc.clear();
    
    modbus.getRegisters(0x04, 0x01, 18);
  
    doc["Year"] = modbus.uint16FromFrame(bigEndian, registers = 3);
    doc["Month"] = modbus.byteFromFrame(registers += 2);
    doc["Day"] = modbus.byteFromFrame(registers += 1);
    doc["Weekday"] = int2weekday(modbus.byteFromFrame(registers += 1));
    doc["Hour"] = modbus.byteFromFrame(registers += 1);
    doc["Minute"] = modbus.byteFromFrame(registers += 1);
    doc["Second"] = modbus.byteFromFrame(registers += 1);
    doc["Millisecond"] = modbus.byteFromFrame(registers += 1);
    doc["TimeZone"] = modbus.int16FromFrame(bigEndian, registers += 1);
    doc["Season"] = modbus.byteFromFrame(registers += 2) == 0x00 ? "Winter" : (0x80 ? "Summer" : "?");
  
    char outChar[12];
    memset(outChar, 0, 12);
    modbus.charFromFrame(outChar, 10, registers += 1);
    doc["DeviceID"] = atol(outChar);
  
    memset(outChar, 0, 12);
    modbus.charFromFrame(outChar, 5, registers += 16);
    doc["Core Firmware"] = outChar;
  
    memset(outChar, 0, 12);
    modbus.charFromFrame(outChar, 5, registers += 5);
    doc["App Firmware"] = outChar;
  
    memset(outChar, 0, 12);
    modbus.charFromFrame(outChar, 5, registers += 5);
    doc["Com Firmware"] = outChar;
  
    doc["Address"] = modbus.byteFromFrame(registers += 5);
  
    if (edpbox.NumberPhases == THREE_PHASE) {
      registers -= 1;
    }
  
    memset(outChar, 0, 12);
    modbus.charFromFrame(outChar, 6, registers += 35);
    doc["Active Calendar"] = outChar;
  
    doc["Active Tariff"] = modbus.byteFromFrame(registers += 6);
  
    doc["+T1"] = modbus.uint32FromFrame(bigEndian, registers += 1);
    doc["+T2"] = modbus.uint32FromFrame(bigEndian, registers += 4);
    doc["+T3"] = modbus.uint32FromFrame(bigEndian, registers += 4);
    doc["+T4"] = modbus.uint32FromFrame(bigEndian, registers += 4);
    doc["+T5"] = modbus.uint32FromFrame(bigEndian, registers += 4);
    doc["+T6"] = modbus.uint32FromFrame(bigEndian, registers += 4);
    doc["PT"] = modbus.uint32FromFrame(bigEndian, registers += 4);
  
    telemetryTopicString = "telemetry/" + edpbox.ThingId + "/energymeter/info";
    telemetryTopicString.toCharArray(telemetryTopicChar, 96);
  
    serializeJson(doc, buffer);
    mqttClient.publish(telemetryTopicChar, buffer);

    digitalWrite(LED_BUILTIN, HIGH);

    edpbox.LAST_INFO_COMMUNICATION = millis();
  }

  if (millis() - edpbox.LAST_TOTAL_COMMUNICATION > REPORTING_TOTAL_PERIOD) {
    
    digitalWrite(LED_BUILTIN, LOW);
    
    mqttClient.publish(debugTopicChar, "Reading Total Registers");
  
    doc.clear();
  
    switch(edpbox.NumberPhases) {
      case(ONE_PHASE):
        modbus.getRegisters(0x04, 0x16, 6);
        break;
      case(THREE_PHASE):
        modbus.getRegisters(0x04, 0x16, 16);
        break;
    }
  
    doc["+A"] = (float)modbus.uint32FromFrame(bigEndian, registers = 3)/1000.0;
    if (edpbox.EnergyExport) {
      doc["-A"] = (float)modbus.uint32FromFrame(bigEndian, registers += 4)/1000.0; 
    } else {
      registers += 4;
    }
    doc["+Ri"] = (float)modbus.uint32FromFrame(bigEndian, registers += 4)/1000.0;
    doc["+Rc"] = (float) modbus.uint32FromFrame(bigEndian, registers += 4)/1000.0;
    if (edpbox.EnergyExport) {
      doc["-Ri"] = (float)modbus.uint32FromFrame(bigEndian, registers += 4)/1000.0;
      doc["-Rc"] = (float)modbus.uint32FromFrame(bigEndian, registers += 4)/1000.0;
    } else {
      registers += 4*2;
    }
  
    if (edpbox.NumberPhases == ONE_PHASE) {
      modbus.getRegisters(0x04, 0x22, 4);
      registers = -1;
    }
  
    doc["QI+QIV"]["P"] = modbus.uint32FromFrame(bigEndian, registers += 4);
    doc["QI+QIV"]["Year"] = modbus.uint16FromFrame(bigEndian, registers += 4);
    doc["QI+QIV"]["Month"] = modbus.byteFromFrame(registers += 2);
    doc["QI+QIV"]["Day"] = modbus.byteFromFrame(registers += 1);
    doc["QI+QIV"]["Weekday"] = int2weekday(modbus.byteFromFrame(registers += 1));
    doc["QI+QIV"]["Hour"] = modbus.byteFromFrame(registers += 1);
    doc["QI+QIV"]["Minute"] = modbus.byteFromFrame(registers += 1);
    doc["QI+QIV"]["Second"] = modbus.byteFromFrame(registers += 1);
    doc["QI+QIV"]["Millisecond"] = modbus.byteFromFrame(registers += 1);
    doc["QI+QIV"]["TimeZone"] = modbus.int16FromFrame(bigEndian, registers += 1);
    doc["QI+QIV"]["Season"] = modbus.byteFromFrame(registers += 2) == 0x00 ? "Winter" : (0x80 ? "Summer" : "?");
  
    if (edpbox.EnergyExport) {
      doc["QII+QIII"]["P"] = modbus.uint32FromFrame(bigEndian, registers += 1);
      doc["QII+QIII"]["Year"] = modbus.uint16FromFrame(bigEndian, registers += 4);
      doc["QII+QIII"]["Month"] = modbus.byteFromFrame(registers += 2);
      doc["QII+QIII"]["Day"] = modbus.byteFromFrame(registers += 1);
      doc["QII+QIII"]["Weekday"] = int2weekday(modbus.byteFromFrame(registers += 1));
      doc["QII+QIII"]["Hour"] = modbus.byteFromFrame(registers += 1);
      doc["QII+QIII"]["Minute"] = modbus.byteFromFrame(registers += 1);
      doc["QII+QIII"]["Second"] = modbus.byteFromFrame(registers += 1);
      doc["QII+QIII"]["Millisecond"] = modbus.byteFromFrame(registers += 1);
      doc["QII+QIII"]["TimeZone"] = modbus.int16FromFrame(bigEndian, registers += 1);
      doc["QII+QIII"]["Season"] = modbus.byteFromFrame(registers += 2) == 0x00 ? "Winter" : (0x80 ? "Summer" : "?");
    }
    
    telemetryTopicString = "telemetry/" + edpbox.ThingId + "/energymeter/total";
    telemetryTopicString.toCharArray(telemetryTopicChar, 96);
  
    serializeJson(doc, buffer);
    mqttClient.publish(telemetryTopicChar, buffer);

    digitalWrite(LED_BUILTIN, HIGH);

    edpbox.LAST_TOTAL_COMMUNICATION = millis();
    
  }

  if (millis() - edpbox.LAST_TARIFF_COMMUNICATION > REPORTING_TARIFF_PERIOD) {
    
    digitalWrite(LED_BUILTIN, LOW);
    
    mqttClient.publish(debugTopicChar, "Reading Tariff Registers");
  
    doc.clear();
    
    modbus.getRegisters(0x04, 0x26, 35);
    char key[16];
    char *keyFirst[] = {"+A", "+Ri", "+Rc", "-Ri", "-Rc"};
    char keyLast[] = {'1', '2', '3', '4', '5', '6', 'T'};
    registers = -1;
    
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 7; j++) {      
        registers += 4;
  
        if (!edpbox.EnergyExport & i > 2) {
          continue;
        } else if (edpbox.NumberTariffs == THREE_TARIFF & (j > 2 & j < 6)) {
          continue;
        }
  
        snprintf(key, 16, "%s_%c", keyFirst[i], keyLast[j]);
        doc[key] = (float)modbus.uint32FromFrame(bigEndian, registers)/1000.0;      
      }
    }
  
    modbus.getRegisters(0x04, 0x50, 0x0E);
    registers = 2;
    char keys3[] = {'1', '2', '3', 'T'};
    for (int i = 0; i < sizeof(keys3); i++) {
      snprintf(key, 16, "+P_%c_MAX", keys3[i]);
      
      doc[key]["P"] = modbus.uint32FromFrame(bigEndian, registers += 1);
      doc[key]["Year"] = modbus.uint16FromFrame(bigEndian, registers += 4);
      doc[key]["Month"] = modbus.byteFromFrame(registers += 2);
      doc[key]["Day"] = modbus.byteFromFrame(registers += 1);
      doc[key]["Weekday"] = int2weekday(modbus.byteFromFrame(registers += 1));
      doc[key]["Hour"] = modbus.byteFromFrame(registers += 1);
      doc[key]["Minute"] = modbus.byteFromFrame(registers += 1);
      doc[key]["Second"] = modbus.byteFromFrame(registers += 1);
      doc[key]["Millisecond"] = modbus.byteFromFrame(registers += 1);
      doc[key]["TimeZone"] = modbus.int16FromFrame(bigEndian, registers += 1);
      doc[key]["Season"] = modbus.byteFromFrame(registers += 2) == 0x00 ? "Winter" : (0x80 ? "Summer" : "?");
  
      if (i == 2) {
        registers += 16*3;
      }
    }
  
    telemetryTopicString = "telemetry/" + edpbox.ThingId + "/energymeter/tariff";
    telemetryTopicString.toCharArray(telemetryTopicChar, 96);
  
    serializeJson(doc, buffer);
    mqttClient.publish(telemetryTopicChar, buffer);

    digitalWrite(LED_BUILTIN, HIGH);

    edpbox.LAST_TARIFF_COMMUNICATION = millis();
    
  }

  if (millis() - edpbox.LAST_INSTANTANEOUS_COMMUNICATION > REPORTING_INSTANTANEOUS_PERIOD) {
    
    digitalWrite(LED_BUILTIN, LOW);
    
    mqttClient.publish(debugTopicChar, "Reading Instantaneous Values");
  
    doc.clear();

    switch(edpbox.NumberPhases) {
      case(ONE_PHASE):
        modbus.getRegisters(0x04, 0x6C, 2);
        break;
      case(THREE_PHASE):
        modbus.getRegisters(0x04, 0x6C, 20);
        break;
    }

    doc["V_L1"] = (float)modbus.uint16FromFrame(bigEndian, registers = 3)/10.0;
    doc["I_L1"] = (float)modbus.uint16FromFrame(bigEndian, registers += 2)/10.0;

    if (edpbox.NumberPhases == THREE_PHASE) {
      doc["V_L2"] = (float)modbus.uint16FromFrame(bigEndian, registers += 2)/10.0;
      doc["I_L2"] = (float)modbus.uint16FromFrame(bigEndian, registers += 2)/10.0;
      doc["V_L3"] = (float)modbus.uint16FromFrame(bigEndian, registers += 2)/10.0;
      doc["I_L3"] = (float)modbus.uint16FromFrame(bigEndian, registers += 2)/10.0;
      doc["I_T"] = (float)modbus.uint16FromFrame(bigEndian, registers += 2)/10.0;
      doc["+P_L1"] = modbus.uint32FromFrame(bigEndian, registers += 2);
      doc["-P_L1"] = modbus.uint32FromFrame(bigEndian, registers += 4);
      doc["+P_L2"] = modbus.uint32FromFrame(bigEndian, registers += 4);
      doc["-P_L2"] = modbus.uint32FromFrame(bigEndian, registers += 4);
      doc["+P_L3"] = modbus.uint32FromFrame(bigEndian, registers += 4);
      doc["-P_L3"] = modbus.uint32FromFrame(bigEndian, registers += 4);
    } else {
      modbus.getRegisters(0x04, 0x79, 3);
      registers = -1;
    }
    
    doc["+P"] = modbus.uint32FromFrame(bigEndian, registers += 4);
    doc["-P"] = modbus.uint32FromFrame(bigEndian, registers += 4);
    doc["PF_T"] = (float)modbus.uint16FromFrame(bigEndian, registers += 4)/1000.0;

    if (edpbox.NumberPhases == THREE_PHASE) {
      doc["PF_L1"] = (float)modbus.uint16FromFrame(bigEndian, registers += 2)/1000.0;
      doc["PF_L2"] = (float)modbus.uint16FromFrame(bigEndian, registers += 2)/1000.0;
      doc["PF_L3"] = (float)modbus.uint16FromFrame(bigEndian, registers += 2)/1000.0;
    } else {
      modbus.getRegisters(0x04, 0x7F, 1);
      registers = 1;
    }
    
    doc["F"] = (float)modbus.uint16FromFrame(bigEndian, registers += 2)/10.0;
    
    telemetryTopicString = "telemetry/" + edpbox.ThingId + "/energymeter/instantaneous";
    telemetryTopicString.toCharArray(telemetryTopicChar, 96);
  
    serializeJson(doc, buffer);
    mqttClient.publish(telemetryTopicChar, buffer);

    digitalWrite(LED_BUILTIN, HIGH);

    edpbox.LAST_INSTANTANEOUS_COMMUNICATION = millis();
    
  }

  return edpbox;
}
