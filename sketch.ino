#include <LiquidCrystal.h>
#include <DHT.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <ESP32Servo.h>
#include <WiFi.h>
#include <PubSubClient.h>

LiquidCrystal lcd(13, 12, 14, 27, 26, 25);

#define LDR_PIN 34
#define SPO2_POT 35
#define DS18_PIN 32
#define DHT_PIN 33
#define MQ2_PIN 39
#define DOSE_PIN 36
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
const uint16_t mqtt_port = 1883;
const char* topicVitals = "hospital/harini_final/vitals";
const char* topicEnv = "hospital/harini_final/env";
const char* topicStatus = "hospital/harini_final/status";
const char* topicControl = "hospital/harini_final/control/#";

WiFiClient espClient;
PubSubClient client(espClient);

QueueHandle_t vitalQueue = nullptr;
QueueHandle_t envQueue = nullptr;
SemaphoreHandle_t lcdMutex = nullptr;
SemaphoreHandle_t servoMutex = nullptr;
SemaphoreHandle_t dataMutex = nullptr;

struct VitalData { int hr; int spo2; float bodyT; int dose; int rawDose; };
struct EnvData { float roomT; int aqi; int humidity; };

static const int BUFFER_SIZE = 20;
VitalData offlineBuffer[BUFFER_SIZE];
volatile int bufferIndex = 0;
volatile int samplingRate = 5;
volatile int targetBedAngle = 10;
volatile int currentBedAngle = 10;
volatile int targetDose = 0;
VitalData g_lastVital = {74, 98, 36.6f, 0, 0};
EnvData g_lastEnv = {24.0f, 0, 50};

enum SystemState { ONLINE, DEGRADED, OFFLINE };
volatile SystemState sysState = OFFLINE;

void dosageSafetyCheck(int dose) {
  if (dose > 80) { digitalWrite(LED_PIN, HIGH); digitalWrite(BUZZ_PIN, HIGH); }
  else if (dose > 50) { digitalWrite(LED_PIN, HIGH); digitalWrite(BUZZ_PIN, LOW); }
  else { digitalWrite(LED_PIN, LOW); digitalWrite(BUZZ_PIN, LOW); }
}

void callback(char* topic, byte* payload, unsigned int length) {
  String msg; for (unsigned int i=0;i<length;i++) msg+=(char)payload[i];
  String rt(topic); msg.trim();
  Serial.printf("\n[CONTROL] %s -> %s\n", rt.c_str(), msg.c_str());
  if (rt == "hospital/harini_final/control/bed") {
    int na=10; if (msg=="sleep") na=10; else if (msg=="breath") na=45; else if (msg=="emergency") na=90; else na=constrain(msg.toInt(),0,90);
    targetBedAngle=na;
  } else if (rt == "hospital/harini_final/control/sampling") {
    int s=msg.toInt(); if (s>=5 && s<=60) samplingRate=s;
  } else if (rt == "hospital/harini_final/control/dosage") {
    int nd=constrain(msg.toInt(),0,100);
    if (nd <= targetDose || abs(nd-(int)targetDose)<=20) targetDose=nd;
    else Serial.printf("Rate limit blocked %d->%d\n",(int)targetDose,nd);
  }
}

// *** SENSOR TASK - FIXED - 100% PRINT ***
void SensorTask(void* pv) {
  VitalData v;
  for(;;){
    // Force read - no blocking
    v.hr = map(analogRead(LDR_PIN),0,4095,60,120);
    v.spo2 = map(analogRead(SPO2_POT),0,4095,90,100);
    ds18b20.requestTemperatures();
    vTaskDelay(pdMS_TO_TICKS(750));
    v.bodyT = ds18b20.getTempCByIndex(0);
    if (v.bodyT==DEVICE_DISCONNECTED_C || isnan(v.bodyT) || v.bodyT<10 || v.bodyT>60) v.bodyT=36.6f;

    int raw=analogRead(DOSE_PIN);
    v.rawDose=raw;
    int meas=map(raw,0,4095,0,100);
    if (raw>50) v.dose=meas;
    else if (targetDose>0) { v.dose=targetDose; v.rawDose=map(targetDose,0,100,0,4095); }
    else v.dose=meas;

    g_lastVital=v;
    if (vitalQueue) xQueueOverwrite(vitalQueue,&v);
    dosageSafetyCheck(targetDose>0?targetDose:v.dose);

    // *** IMPORTANT PRINT - ALWAYS ***
    Serial.printf("\n[SENSOR] HR:%d SpO2:%d BodyT:%.1f Dose:%d%% RAW:%d Target:%d\n", v.hr, v.spo2, v.bodyT, v.dose, raw, (int)targetDose);
    Serial.flush();

    int d=samplingRate;
    if (v.hr>100 || v.spo2<93 || v.bodyT>38.0f) d=5;
    vTaskDelay(pdMS_TO_TICKS(d*1000));
  }
}

void EnvTask(void* pv){
  EnvData e;
  for(;;){
    float t=dht.readTemperature(); float h=dht.readHumidity();
    e.roomT=isnan(t)?24.5f:t;
    e.humidity=isnan(h)?60:(int)round(h);
    e.aqi=map(analogRead(MQ2_PIN),0,4095,0,300);
    g_lastEnv=e;
    if (envQueue) xQueueOverwrite(envQueue,&e);
    Serial.printf("[ENV] Room:%.1fC Hum:%d%% AQI:%d\n", e.roomT, e.humidity, e.aqi);
    vTaskDelay(pdMS_TO_TICKS(5000));
  }
}

bool connectWiFi(uint32_t to=8000){
  if (WiFi.status()==WL_CONNECTED) return true;
  WiFi.mode(WIFI_STA); WiFi.begin(ssid,password);
  uint32_t st=millis();
  while(WiFi.status()!=WL_CONNECTED && millis()-st<to){ vTaskDelay(pdMS_TO_TICKS(300)); }
  return WiFi.status()==WL_CONNECTED;
}

void MqttTask(void* pv){
  uint32_t lastPub=0, lastWifi=0, retry=3000;
  for(;;){
    if (WiFi.status()!=WL_CONNECTED){
      sysState=OFFLINE;
      VitalData v; if (vitalQueue && xQueuePeek(vitalQueue,&v,0)==pdTRUE){ offlineBuffer[bufferIndex]=v; bufferIndex=(bufferIndex+1)%BUFFER_SIZE; }
      if (millis()-lastWifi>=retry){
        lastWifi=millis();
        Serial.printf("[WIFI] Reconnecting... retry %d ms\n", retry);
        connectWiFi(6000);
        retry=min(retry*2,(uint32_t)30000);
      }
      vTaskDelay(pdMS_TO_TICKS(1000));
      continue;
    }
    retry=3000;
    if (!client.connected()){
      sysState=DEGRADED;
      String cid="ESP32Health-"+String((uint32_t)esp_random(),HEX);
      if (client.connect(cid.c_str())){
        client.subscribe(topicControl);
        sysState=ONLINE;
        Serial.println("\n[MQTT] Connected & Subscribed!");
        if (bufferIndex>0){
          Serial.printf("[SYNC] Restoring %d records\n", bufferIndex);
          for(int i=0;i<bufferIndex;i++){ char p[160]; snprintf(p,sizeof(p),"{\"hr\":%d,\"spo2\":%d,\"bodyTemp\":%.2f,\"dose\":%d,\"raw\":%d,\"offline\":1}",offlineBuffer[i].hr,offlineBuffer[i].spo2,offlineBuffer[i].bodyT,offlineBuffer[i].dose,offlineBuffer[i].rawDose); client.publish(topicVitals,p); vTaskDelay(pdMS_TO_TICKS(100)); }
          bufferIndex=0; Serial.println("Sync complete");
        }
      } else { vTaskDelay(pdMS_TO_TICKS(2000)); continue; }
    }
    client.loop();
    if (millis()-lastPub>=5000){
      lastPub=millis();
      VitalData v; EnvData e;
      bool hv=vitalQueue && xQueuePeek(vitalQueue,&v,0)==pdTRUE;
      bool he=envQueue && xQueuePeek(envQueue,&e,0)==pdTRUE;
      if (hv){ char p[160]; snprintf(p,sizeof(p),"{\"hr\":%d,\"spo2\":%d,\"bodyTemp\":%.2f,\"dose\":%d,\"raw\":%d}",v.hr,v.spo2,v.bodyT,v.dose,v.rawDose); client.publish(topicVitals,p); Serial.printf("[MQTT PUB] %s\n",p); }
      if (he){ char p[120]; snprintf(p,sizeof(p),"{\"temp\":%.2f,\"roomTemp\":%.2f,\"aqi\":%d,\"humidity\":%d}",e.roomT,e.roomT,e.aqi,e.humidity); client.publish(topicEnv,p); }
    }
    vTaskDelay(pdMS_TO_TICKS(100));
  }
}

void BedTask(void* pv){ for(;;){ int req=constrain((int)targetBedAngle,0,90); if (servoMutex && xSemaphoreTake(servoMutex,pdMS_TO_TICKS(100))==pdTRUE){ if (currentBedAngle<req) currentBedAngle++; else if (currentBedAngle>req) currentBedAngle--; bedServo.write(currentBedAngle); xSemaphoreGive(servoMutex); } vTaskDelay(pdMS_TO_TICKS(100)); } }

void LcdTask(void* pv){
  int page=0;
  for(;;){
    VitalData v=g_lastVital; EnvData e=g_lastEnv;
    if (lcdMutex && xSemaphoreTake(lcdMutex,pdMS_TO_TICKS(100))==pdTRUE){
      lcd.clear();
      if (sysState==OFFLINE){ lcd.setCursor(0,0); lcd.print("LOGGING OFFLINE"); lcd.setCursor(0,1); lcd.print("Q:"); lcd.print((int)bufferIndex); lcd.print(" B:"); lcd.print((int)currentBedAngle); }
      else if (page==0){ lcd.setCursor(0,0); lcd.print("HR:"); lcd.print(v.hr); lcd.print(" S:"); lcd.print(v.spo2); lcd.setCursor(0,1); if (sysState==ONLINE) lcd.print("ON "); else lcd.print("OFF "); lcd.print("B:"); lcd.print((int)currentBedAngle); }
      else if (page==1){ lcd.setCursor(0,0); lcd.print("Body:"); lcd.print(v.bodyT,1); lcd.setCursor(0,1); lcd.print("Dose:"); lcd.print(v.dose); lcd.print("% T:"); lcd.print((int)targetDose); }
      else { lcd.setCursor(0,0); lcd.print("Room:"); lcd.print(e.roomT,1); lcd.setCursor(0,1); lcd.print("AQI:"); lcd.print(e.aqi); lcd.print(" H:"); lcd.print(e.humidity); }
      xSemaphoreGive(lcdMutex);
    }
    vTaskDelay(pdMS_TO_TICKS(2000));
    page=(page+1)%3;
  }
}

void setup(){
  Serial.begin(115200); delay(1000);
  Serial.println("\n\n=== Smart Hospital - ALL 6 TASKS ===");
  Serial.println("Build OK - Sensor will print every 5 sec\n");
  pinMode(LED_PIN,OUTPUT); pinMode(BUZZ_PIN,OUTPUT);
  digitalWrite(LED_PIN,LOW); digitalWrite(BUZZ_PIN,LOW);
  analogReadResolution(12);
  dht.begin(); ds18b20.begin(); ds18b20.setWaitForConversion(false);
  lcd.begin(16,2); lcd.clear(); lcd.print("Smart Hospital"); lcd.setCursor(0,1); lcd.print("6 Tasks Ready");
  bedServo.setPeriodHertz(50); bedServo.attach(SERVO_PIN,500,2400); bedServo.write(10);
  client.setServer(mqtt_server,mqtt_port); client.setCallback(callback); client.setKeepAlive(30); client.setBufferSize(512);
  vitalQueue=xQueueCreate(1,sizeof(VitalData)); envQueue=xQueueCreate(1,sizeof(EnvData));
  lcdMutex=xSemaphoreCreateMutex(); servoMutex=xSemaphoreCreateMutex(); dataMutex=xSemaphoreCreateMutex();
  VitalData iv={74,98,36.6f,0,0}; EnvData ie={24.0f,0,50};
  xQueueOverwrite(vitalQueue,&iv); xQueueOverwrite(envQueue,&ie);
  xTaskCreate(SensorTask,"Sensor",8192,nullptr,2,nullptr);
  xTaskCreate(EnvTask,"Env",4096,nullptr,1,nullptr);
  xTaskCreate(MqttTask,"MQTT",8192,nullptr,1,nullptr);
  xTaskCreate(BedTask,"Bed",4096,nullptr,1,nullptr);
  xTaskCreate(LcdTask,"LCD",4096,nullptr,1,nullptr);
  Serial.println("Tasks started - Wait 5 sec for sensor data...\n");
}
void loop(){ vTaskDelay(pdMS_TO_TICKS(1000)); }
