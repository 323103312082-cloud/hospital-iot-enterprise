#include <LiquidCrystal.h>
#include <DHT.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <ESP32Servo.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <freertos/queue.h>
#include <freertos/semphr.h>

LiquidCrystal lcd(13, 12, 14, 27, 26, 25);
#define LDR_PIN 34
#define SPO2_POT 35
#define DS18_PIN 32
#define DHT_PIN 33
#define MQ2_PIN 4
#define DOSE_PIN 15
#define SERVO_PIN 18
#define LED_PIN 23
#define BUZZ_PIN 2
#define DHTTYPE DHT22
DHT dht(DHT_PIN, DHTTYPE);
OneWire oneWire(DS18_PIN);
DallasTemperature ds18b20(&oneWire);
Servo bedServo;

const char* ssid = "Wokwi-GUEST";
const char* password = "";
const char* mqtt_server = "broker.emqx.io";
WiFiClient espClient;
PubSubClient client(espClient);

QueueHandle_t vitalQueue;
QueueHandle_t envQueue;
SemaphoreHandle_t lcdMutex;
SemaphoreHandle_t servoMutex;

struct VitalData { int hr; int spo2; float bodyT; int dose; };
struct EnvData { float roomT; int aqi; };

int samplingRate = 5;
int targetBedAngle = 0;
int currentBedAngle = 0;
int lastDose = 0;
enum SystemState { ONLINE, DEGRADED, OFFLINE };
SystemState sysState = ONLINE;

#define BUFFER_SIZE 20
VitalData offlineBuffer[BUFFER_SIZE];
int bufferIndex = 0;

void callback(char* topic, byte* payload, unsigned int length) {
  String msg;
  for(int i=0;i<length;i++) msg += (char)payload[i];
  if(String(topic)=="hospital/control/dosage"){
    int newDose = msg.toInt();
    if(abs(newDose - lastDose) > 20) return;
    lastDose = newDose;
  }
  if(String(topic)=="hospital/control/bed"){
    if(msg=="sleep") targetBedAngle = 10;
    else if(msg=="breath") targetBedAngle = 45;
    else if(msg=="emergency") targetBedAngle = 90;
    else targetBedAngle = constrain(msg.toInt(),0,90);
  }
  if(String(topic)=="hospital/control/sampling"){
    samplingRate = constrain(msg.toInt(),5,60);
  }
}

void SensorTask(void *pv){
  VitalData v;
  for(;;){
    v.hr = map(analogRead(LDR_PIN), 0, 4095, 60, 120);
    v.spo2 = map(analogRead(SPO2_POT), 0, 4095, 90, 100);
    ds18b20.requestTemperatures();
    v.bodyT = ds18b20.getTempCByIndex(0);
    if(v.bodyT == -127) v.bodyT = 36.6;
    v.dose = map(analogRead(DOSE_PIN), 0, 4095, 0, 100);
    if(abs(v.dose - lastDose) > 20) v.dose = lastDose;
    else lastDose = v.dose;
    xQueueSend(vitalQueue, &v, 0);
    int dynamicDelay = samplingRate;
    if(v.hr > 100 || v.spo2 < 93 || v.bodyT > 38) dynamicDelay = 5;
    vTaskDelay(dynamicDelay * 1000 / portTICK_PERIOD_MS);
  }
}

void EnvTask(void *pv){
  EnvData e;
  for(;;){
    e.roomT = dht.readTemperature();
    if(isnan(e.roomT)) e.roomT = 26;
    e.aqi = map(analogRead(MQ2_PIN), 0, 4095, 0, 300);
    xQueueSend(envQueue, &e, 0);
    vTaskDelay(5000 / portTICK_PERIOD_MS);
  }
}

void MqttTask(void *pv){
  for(;;){
    if(WiFi.status()!=WL_CONNECTED ||!client.connected()){
      sysState = (WiFi.status()!=WL_CONNECTED)? OFFLINE : DEGRADED;
      VitalData v;
      if(xQueuePeek(vitalQueue,&v,0)==pdTRUE && bufferIndex < BUFFER_SIZE){
        offlineBuffer[bufferIndex++] = v;
      }
      if(WiFi.status()!=WL_CONNECTED) WiFi.begin(ssid,password);
      else {
        client.connect("ESP32Health");
        client.subscribe("hospital/control/#");
      }
    } else {
      sysState = ONLINE;
      if(bufferIndex > 0){
        for(int i=0;i<bufferIndex;i++){
          String payload = String(offlineBuffer[i].hr) + "," + String(offlineBuffer[i].spo2);
          client.publish("hospital/patient/vitals_sync", payload.c_str());
          vTaskDelay(100 / portTICK_PERIOD_MS);
        }
        bufferIndex = 0;
      }
      VitalData v; EnvData e;
      if(xQueueReceive(vitalQueue,&v,0)==pdTRUE){
        String p = "{\"hr\":"+String(v.hr)+",\"spo2\":"+String(v.spo2)+",\"bodyT\":"+String(v.bodyT)+",\"dose\":"+String(v.dose)+"}";
        client.publish("hospital/patient/vitals", p.c_str());
      }
      if(xQueueReceive(envQueue,&e,0)==pdTRUE){
        String p = "{\"roomT\":"+String(e.roomT)+",\"aqi\":"+String(e.aqi)+"}";
        client.publish("hospital/room/env", p.c_str());
      }
    }
    client.loop();
    vTaskDelay(1000 / portTICK_PERIOD_MS);
  }
}

void BedTask(void *pv){
  for(;;){
    VitalData v;
    xQueuePeek(vitalQueue,&v,0);
    if(v.spo2 < 93) targetBedAngle = 45;
    if(v.hr > 110) targetBedAngle = 90;
    xSemaphoreTake(servoMutex, portMAX_DELAY);
    if(currentBedAngle < targetBedAngle) currentBedAngle++;
    else if(currentBedAngle > targetBedAngle) currentBedAngle--;
    bedServo.write(currentBedAngle);
    xSemaphoreGive(servoMutex);
    vTaskDelay(100 / portTICK_PERIOD_MS);
  }
}

void DisplayTask(void *pv){
  VitalData v;
  for(;;){
    xSemaphoreTake(lcdMutex, portMAX_DELAY);
    lcd.clear();
    if(sysState == OFFLINE){
      lcd.print("LOGGING OFFLINE");
      lcd.setCursor(0,1);
      lcd.print("Buffer:"); lcd.print(bufferIndex);
    } else {
      if(xQueuePeek(vitalQueue,&v,0)==pdTRUE){
        lcd.setCursor(0,0);
        lcd.print("H:"); lcd.print(v.hr);
        lcd.print(" S:"); lcd.print(v.spo2);
        lcd.setCursor(0,1);
        lcd.print("D:"); lcd.print(v.dose);
        lcd.print(" SR:"); lcd.print(samplingRate);
      }
    }
    xSemaphoreGive(lcdMutex);
    vTaskDelay(2000 / portTICK_PERIOD_MS);
  }
}

void AlertTask(void *pv){
  for(;;){
    VitalData v;
    if(xQueuePeek(vitalQueue,&v,0)==pdTRUE){
      EnvData e; xQueuePeek(envQueue,&e,0);
      bool critical = (v.hr>100 || v.spo2<93 || v.bodyT>38 || e.aqi>150 || v.dose>80);
      if(critical){
        if(v.dose>80){
          for(int i=0;i<5;i++){
            digitalWrite(LED_PIN,HIGH); vTaskDelay(100 / portTICK_PERIOD_MS);
            digitalWrite(LED_PIN,LOW); vTaskDelay(100 / portTICK_PERIOD_MS);
          }
          digitalWrite(BUZZ_PIN,HIGH);
        } else {
          digitalWrite(LED_PIN,HIGH);
          digitalWrite(BUZZ_PIN,HIGH);
        }
      } else {
        digitalWrite(LED_PIN,LOW);
        digitalWrite(BUZZ_PIN,LOW);
      }
    }
    vTaskDelay(500 / portTICK_PERIOD_MS);
  }
}

void setup(){
  Serial.begin(115200);
  lcd.begin(16,2);
  dht.begin();
  ds18b20.begin();
  bedServo.attach(SERVO_PIN);
  pinMode(LED_PIN,OUTPUT);
  pinMode(BUZZ_PIN,OUTPUT);
  bedServo.write(0);

  WiFi.begin(ssid,password);
  client.setServer(mqtt_server,1883);
  client.setCallback(callback);

  vitalQueue = xQueueCreate(10, sizeof(VitalData));
  envQueue = xQueueCreate(10, sizeof(EnvData));
  lcdMutex = xSemaphoreCreateMutex();
  servoMutex = xSemaphoreCreateMutex();

  xTaskCreate(SensorTask, "Sensor", 4096, NULL, 2, NULL);
  xTaskCreate(EnvTask, "Env", 2048, NULL, 2, NULL);
  xTaskCreate(MqttTask, "MQTT", 4096, NULL, 1, NULL);
  xTaskCreate(BedTask, "Bed", 2048, NULL, 2, NULL);
  xTaskCreate(DisplayTask, "Display", 2048, NULL, 1, NULL);
  xTaskCreate(AlertTask, "Alert", 2048, NULL, 3, NULL);

  lcd.print("FreeRTOS System");
  lcd.setCursor(0,1);
  lcd.print("6 Tasks Running");
  vTaskDelay(2000 / portTICK_PERIOD_MS);
  lcd.clear();
}

void loop(){
  vTaskDelay(1000 / portTICK_PERIOD_MS);
}
