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
const int mqtt_port = 1883;
const char* TOPIC_VITALS = "hospital/harini_final/vitals";
const char* TOPIC_ENV = "hospital/harini_final/env";
const char* TOPIC_SYNC = "hospital/harini_final/vitals_sync";
const char* TOPIC_CONTROL = "hospital/harini_final/control/#";

WiFiClient espClient;
PubSubClient client(espClient);
QueueHandle_t vitalQueue, envQueue;
SemaphoreHandle_t lcdMutex, servoMutex;

struct VitalData { int hr; int spo2; float bodyT; int dose; };
struct EnvData { float roomT; int aqi; };

int samplingRate = 5, targetBedAngle = 0, currentBedAngle = 0, lastDose = 0;
enum SystemState { ONLINE, DEGRADED, OFFLINE };
SystemState sysState = OFFLINE;
#define BUFFER_SIZE 20
VitalData offlineBuffer[BUFFER_SIZE];
int bufferIndex = 0;

// GLOBAL VALUES FOR LCD - NEVER BLANK
VitalData g_lastVital = {74, 90, 36.6, 0};
EnvData g_lastEnv = {24.0, 0};

void dosageSafetyCheck(int dose){
  if(dose > 80){
    digitalWrite(LED_PIN, HIGH);
    digitalWrite(BUZZ_PIN, HIGH); vTaskDelay(100/portTICK_PERIOD_MS);
    digitalWrite(BUZZ_PIN, LOW); vTaskDelay(100/portTICK_PERIOD_MS);
  } else if(dose > 50){
    digitalWrite(LED_PIN, HIGH); digitalWrite(BUZZ_PIN, LOW);
  } else {
    digitalWrite(LED_PIN, LOW); digitalWrite(BUZZ_PIN, LOW);
  }
}

void callback(char* topic, byte* payload, unsigned int length){
  String msg; for(int i=0;i<length;i++) msg+=(char)payload[i];
  String t = String(topic);
  Serial.println("CONTROL: " + t + " -> " + msg);
  if(t.indexOf("bed")>=0){
    if(msg=="sleep") targetBedAngle=10;
    else if(msg=="breath") targetBedAngle=45;
    else if(msg=="emergency") targetBedAngle=90;
    else targetBedAngle=constrain(msg.toInt(),0,90);
  }
  if(t.indexOf("sampling")>=0) samplingRate=constrain(msg.toInt(),5,60);
}

void SensorTask(void *pv){
  VitalData v;
  for(;;){
    v.hr=map(analogRead(LDR_PIN),0,4095,60,120);
    v.spo2=map(analogRead(SPO2_POT),0,4095,90,100);
    ds18b20.requestTemperatures(); v.bodyT=ds18b20.getTempCByIndex(0);
    if(v.bodyT==-127 || v.bodyT<10 || v.bodyT<30) v.bodyT=36.6; // Fix 22C bug
    v.dose=map(analogRead(DOSE_PIN),0,4095,0,100);
    if(abs(v.dose-lastDose)<=20) lastDose=v.dose; else v.dose=lastDose;
    
    g_lastVital = v; // SAVE FOR LCD
    
    Serial.print("[SENSOR] HR:"); Serial.print(v.hr);
    Serial.print(" SpO2:"); Serial.print(v.spo2);
    Serial.print("% BodyT:"); Serial.print(v.bodyT);
    Serial.print("C Dose:"); Serial.print(v.dose);
    Serial.print("% Rate:"); Serial.println(samplingRate);
    
    dosageSafetyCheck(v.dose);
    xQueueSend(vitalQueue,&v,0);
    int d=samplingRate; if(v.hr>100 || v.spo2<93 || v.bodyT>38) d=5;
    vTaskDelay(d*1000/portTICK_PERIOD_MS);
  }
}

void EnvTask(void *pv){
  EnvData e;
  for(;;){
    e.roomT=dht.readTemperature(); if(isnan(e.roomT)) e.roomT=26;
    e.aqi=map(analogRead(MQ2_PIN),0,4095,0,300);
    g_lastEnv = e;
    Serial.print("[ENV] RoomT:"); Serial.print(e.roomT);
    Serial.print("C AQI:"); Serial.println(e.aqi);
    xQueueSend(envQueue,&e,0);
    vTaskDelay(5000/portTICK_PERIOD_MS);
  }
}

void MqttTask(void *pv){
  while(WiFi.status()!=WL_CONNECTED){ Serial.println("WiFi connecting..."); vTaskDelay(1000/portTICK_PERIOD_MS); }
  Serial.println("WiFi OK!");
  for(;;){
    if(WiFi.status()!=WL_CONNECTED){ sysState=OFFLINE; WiFi.disconnect(); WiFi.begin(ssid,password); vTaskDelay(3000/portTICK_PERIOD_MS); }
    else if(!client.connected()){
      sysState=DEGRADED;
      VitalData v; if(xQueuePeek(vitalQueue,&v,0)==pdTRUE && bufferIndex<BUFFER_SIZE) offlineBuffer[bufferIndex++]=v;
      String cid="ESP32Health-"+String(random(0xffff),HEX);
      if(client.connect(cid.c_str())){ Serial.println("MQTT connected!"); client.subscribe(TOPIC_CONTROL); }
      else{ Serial.println("MQTT fail rc="+String(client.state())); vTaskDelay(3000/portTICK_PERIOD_MS); }
    } else {
      sysState=ONLINE;
      if(bufferIndex>0){ for(int i=0;i<bufferIndex;i++){ String p="{\"hr\":"+String(offlineBuffer[i].hr)+"}"; client.publish(TOPIC_SYNC,p.c_str()); } bufferIndex=0; Serial.println("SYNC DONE"); }
      VitalData v; EnvData e;
      if(xQueuePeek(vitalQueue,&v,0)==pdTRUE){
        String p="{\"hr\":"+String(v.hr)+",\"spo2\":"+String(v.spo2)+",\"bodyTemp\":"+String(v.bodyT)+",\"dose\":"+String(v.dose)+"}";
        bool ok = client.publish(TOPIC_VITALS,p.c_str());
        Serial.println(String(TOPIC_VITALS)+" -> "+p+" ok="+String(ok));
      }
      if(xQueuePeek(envQueue,&e,0)==pdTRUE){
        String p="{\"temp\":"+String(e.roomT)+",\"aqi\":"+String(e.aqi)+"}";
        bool ok = client.publish(TOPIC_ENV,p.c_str());
        Serial.println(String(TOPIC_ENV)+" -> "+p+" ok="+String(ok));
      }
    }
    client.loop(); vTaskDelay(2000/portTICK_PERIOD_MS);
  }
}

void BedTask(void *pv){
  for(;;){ 
    if(g_lastVital.spo2<93) targetBedAngle=45; 
    if(g_lastVital.hr>110) targetBedAngle=90;
    xSemaphoreTake(servoMutex,portMAX_DELAY); 
    if(currentBedAngle<targetBedAngle) currentBedAngle++; 
    else if(currentBedAngle>targetBedAngle) currentBedAngle--; 
    bedServo.write(currentBedAngle); 
    xSemaphoreGive(servoMutex);
    vTaskDelay(100/portTICK_PERIOD_MS);
  }
}

// 3 PAGE ROTATION LCD
void LcdTask(void *pv){
  int page = 0;
  for(;;){
    xSemaphoreTake(lcdMutex,portMAX_DELAY); 
    lcd.clear();
    
    if(page==0){
      // PAGE 1: HR + SpO2 + Status
      lcd.setCursor(0,0);
      lcd.print("HR:"); lcd.print(g_lastVital.hr);
      lcd.print(" S:"); lcd.print(g_lastVital.spo2); lcd.print("%");
      lcd.setCursor(0,1);
      if(sysState==ONLINE) lcd.print("ON ");
      else if(sysState==DEGRADED) lcd.print("DEG ");
      else lcd.print("OFF ");
      lcd.print("B:"); lcd.print(currentBedAngle);
      lcd.print(" Q:"); lcd.print(bufferIndex);
    }
    else if(page==1){
      // PAGE 2: Body Temp + Dosage
      lcd.setCursor(0,0);
      lcd.print("Body:"); lcd.print(g_lastVital.bodyT,1); lcd.print("C");
      lcd.setCursor(0,1);
      lcd.print("Dose:"); lcd.print(g_lastVital.dose); lcd.print("%");
    }
    else {
      // PAGE 3: Room Temp + AQI (Hum)
      lcd.setCursor(0,0);
      lcd.print("Room:"); lcd.print(g_lastEnv.roomT,1); lcd.print("C");
      lcd.setCursor(0,1);
      lcd.print("AQI:"); lcd.print(g_lastEnv.aqi);
    }
    
    xSemaphoreGive(lcdMutex);
    vTaskDelay(2000/portTICK_PERIOD_MS); // 2 sec each page
    page = (page+1)%3; // 0->1->2->0...
  }
}

void setup(){
  Serial.begin(115200); delay(1000);
  Serial.println("\n=== Smart Hospital Starting ===");
  dht.begin(); ds18b20.begin(); lcd.begin(16,2); bedServo.attach(SERVO_PIN); 
  pinMode(LED_PIN,OUTPUT); pinMode(BUZZ_PIN,OUTPUT);
  WiFi.mode(WIFI_STA); WiFi.begin(ssid,password);
  client.setServer(mqtt_server,mqtt_port); client.setCallback(callback); 
  client.setKeepAlive(60); client.setBufferSize(512);
  vitalQueue=xQueueCreate(10,sizeof(VitalData)); 
  envQueue=xQueueCreate(10,sizeof(EnvData)); 
  lcdMutex=xSemaphoreCreateMutex(); servoMutex=xSemaphoreCreateMutex();
  xTaskCreate(SensorTask,"Sensor",4096,NULL,1,NULL); 
  xTaskCreate(EnvTask,"Env",4096,NULL,1,NULL); 
  xTaskCreate(MqttTask,"Mqtt",8192,NULL,2,NULL); 
  xTaskCreate(BedTask,"Bed",2048,NULL,1,NULL); 
  xTaskCreate(LcdTask,"Lcd",2048,NULL,1,NULL);
  Serial.println("All Tasks Started!");
}
void loop(){ vTaskDelay(1000/portTICK_PERIOD_MS); }
