#include <Arduino.h>
#include <LiquidCrystal.h>
#include <DHT.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <ESP32Servo.h>
#include <WiFi.h>
#include <PubSubClient.h>

// ---------- LCD ----------
LiquidCrystal lcd(13, 12, 14, 27, 26, 25);

// ---------- ESP32 PINS ----------
#define LDR_PIN    34
#define SPO2_POT   35
#define DS18_PIN   32
#define DHT_PIN    33
#define MQ2_PIN    39
#define DOSE_PIN   36
#define SERVO_PIN  18
#define LED_PIN    23
#define BUZZ_PIN   2
#define DHTTYPE    DHT22

// ---------- SENSORS ----------
DHT dht(DHT_PIN, DHTTYPE);
OneWire oneWire(DS18_PIN);
DallasTemperature ds18b20(&oneWire);
Servo bedServo;

// ---------- WIFI & MQTT ----------
const char* ssid = "Wokwi-GUEST";
const char* password = "";
const char* mqtt_server = "broker.emqx.io";
const uint16_t mqtt_port = 1883;

const char* topicVitals =
  "hospital/harini_final/vitals";

const char* topicEnv =
  "hospital/harini_final/env";

const char* topicStatus =
  "hospital/harini_final/status";

const char* topicControl =
  "hospital/harini_final/control/#";

WiFiClient espClient;
PubSubClient client(espClient);

// ---------- FREERTOS OBJECTS ----------
QueueHandle_t vitalQueue = nullptr;
QueueHandle_t envQueue = nullptr;

SemaphoreHandle_t lcdMutex = nullptr;
SemaphoreHandle_t servoMutex = nullptr;
SemaphoreHandle_t dataMutex = nullptr;

// ---------- DATA STRUCTURES ----------
struct VitalData {
  int hr;
  int spo2;
  float bodyT;
  int dose;
};

struct EnvData {
  float roomT;
  int aqi;
  int humidity;
};

static const int BUFFER_SIZE = 20;

VitalData offlineBuffer[BUFFER_SIZE];
volatile int bufferIndex = 0;

volatile int samplingRate = 5;
volatile int targetBedAngle = 10;
volatile int currentBedAngle = 10;
volatile int targetDose = 0;
volatile int lastDose = 0;

VitalData g_lastVital = {74, 98, 36.6f, 0};
EnvData g_lastEnv = {24.0f, 0, 50};

enum SystemState {
  ONLINE,
  DEGRADED,
  OFFLINE
};

volatile SystemState sysState = OFFLINE;

// ---------- SAFETY OUTPUTS ----------
void dosageSafetyCheck(int dose) {
  if (dose > 80) {
    digitalWrite(LED_PIN, HIGH);
    digitalWrite(BUZZ_PIN, HIGH);
  }
  else if (dose > 50) {
    digitalWrite(LED_PIN, HIGH);
    digitalWrite(BUZZ_PIN, LOW);
  }
  else {
    digitalWrite(LED_PIN, LOW);
    digitalWrite(BUZZ_PIN, LOW);
  }
}

// ---------- MQTT CALLBACK ----------
void callback(char* topic, byte* payload,
              unsigned int length) {

  String msg;
  msg.reserve(length);

  for (unsigned int i = 0; i < length; i++) {
    msg += (char)payload[i];
  }

  String receivedTopic(topic);
  msg.trim();

  Serial.printf("[CONTROL] %s -> %s\n",
                receivedTopic.c_str(),
                msg.c_str());

  // Bed control
  if (receivedTopic ==
      "hospital/harini_final/control/bed") {

    if (msg == "sleep") {
      targetBedAngle = 10;
    }
    else if (msg == "breath") {
      targetBedAngle = 45;
    }
    else if (msg == "emergency") {
      targetBedAngle = 90;
    }
    else {
      targetBedAngle = constrain(msg.toInt(), 0, 90);
    }

    Serial.printf("Target bed angle: %d\n",
                  (int)targetBedAngle);
  }

  // Sampling control
  else if (receivedTopic ==
           "hospital/harini_final/control/sampling") {

    int seconds = msg.toInt();

    if (seconds >= 5 && seconds <= 60) {
      samplingRate = seconds;

      Serial.printf("Sampling rate: %d seconds\n",
                    (int)samplingRate);
    }
    else {
      Serial.println("Invalid sampling interval");
    }
  }

  // Optional dosage control
  else if (receivedTopic ==
           "hospital/harini_final/control/dosage") {

    targetDose = constrain(msg.toInt(), 0, 100);

    Serial.printf("Target dose: %d\n",
                  (int)targetDose);
  }
}

// ---------- SENSOR TASK ----------
void SensorTask(void* pvParameters) {
  VitalData v;

  for (;;) {

    // Simulated heart rate
    v.hr = map(
      analogRead(LDR_PIN), 0, 4095, 60, 120
    );

    // Simulated SpO2
    v.spo2 = map(
      analogRead(SPO2_POT), 0, 4095, 90, 100
    );

    // DS18B20 temperature
    ds18b20.requestTemperatures();

    vTaskDelay(pdMS_TO_TICKS(750));

    v.bodyT = ds18b20.getTempCByIndex(0);

    if (v.bodyT == DEVICE_DISCONNECTED_C ||
        isnan(v.bodyT) ||
        v.bodyT < 10 ||
        v.bodyT > 60) {

      v.bodyT = 36.6f;
    }

    // Dose input
    int rawDose = analogRead(DOSE_PIN);

    int measuredDose = map(
      rawDose, 0, 4095, 0, 100
    );

    if (abs(measuredDose - (int)lastDose) <= 20) {
      lastDose = measuredDose;
    }
    else {
      measuredDose = lastDose;
    }

    v.dose = measuredDose;

    // Demo-only dosage simulation
    if (rawDose < 50 && targetDose > 0) {
      v.dose = targetDose;
    }

    if (dataMutex &&
        xSemaphoreTake(
          dataMutex, pdMS_TO_TICKS(50)
        ) == pdTRUE) {

      g_lastVital = v;
      xSemaphoreGive(dataMutex);
    }

    dosageSafetyCheck(
      targetDose > 0 ? targetDose : v.dose
    );

    if (vitalQueue &&
        xQueueOverwrite(vitalQueue, &v) != pdPASS) {

      Serial.println("Vital queue write failed");
    }

    Serial.printf(
      "[SENSOR] HR:%d SpO2:%d BodyT:%.1f Dose:%d\n",
      v.hr, v.spo2, v.bodyT, v.dose
    );

    int delaySeconds = samplingRate;

    if (v.hr > 100 ||
        v.spo2 < 93 ||
        v.bodyT > 38.0f) {

      delaySeconds = 5;
    }

    vTaskDelay(
      pdMS_TO_TICKS(delaySeconds * 1000)
    );
  }
}

// ---------- ENVIRONMENT TASK ----------
void EnvTask(void* pvParameters) {
  EnvData e;

  for (;;) {

    float temp = dht.readTemperature();
    float humidity = dht.readHumidity();

    e.roomT = isnan(temp) ? 24.0f : temp;

    e.humidity = isnan(humidity)
      ? 50
      : (int)round(humidity);

    e.aqi = map(
      analogRead(MQ2_PIN), 0, 4095, 0, 300
    );

    if (dataMutex &&
        xSemaphoreTake(
          dataMutex, pdMS_TO_TICKS(50)
        ) == pdTRUE) {

      g_lastEnv = e;
      xSemaphoreGive(dataMutex);
    }

    if (envQueue &&
        xQueueOverwrite(envQueue, &e) != pdPASS) {

      Serial.println("Environment queue write failed");
    }

    Serial.printf(
      "[ENV] Room:%.1f Humidity:%d AQI:%d\n",
      e.roomT, e.humidity, e.aqi
    );

    vTaskDelay(pdMS_TO_TICKS(5000));
  }
}

// ---------- WIFI CONNECTION ----------
bool connectWiFi(uint32_t timeoutMs = 15000) {

  if (WiFi.status() == WL_CONNECTED) {
    return true;
  }

  Serial.printf("Connecting to WiFi: %s\n", ssid);

  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);

  uint32_t startTime = millis();

  while (WiFi.status() != WL_CONNECTED &&
         millis() - startTime < timeoutMs) {

    vTaskDelay(pdMS_TO_TICKS(500));
    Serial.print(".");
  }

  Serial.println();

  if (WiFi.status() == WL_CONNECTED) {

    Serial.println("WiFi connected!");

    Serial.print("IP Address: ");
    Serial.println(WiFi.localIP());

    return true;
  }

  Serial.printf(
    "WiFi connection failed. Status=%d\n",
    (int)WiFi.status()
  );

  return false;
}

// ---------- MQTT TASK ----------
void MqttTask(void* pvParameters) {

  uint32_t lastPublish = 0;
  uint32_t lastWifiAttempt = 0;

  for (;;) {

    // Check WiFi connection
    if (WiFi.status() != WL_CONNECTED) {

      sysState = OFFLINE;

      if (millis() - lastWifiAttempt >= 3000) {
        lastWifiAttempt = millis();
        connectWiFi(10000);
      }

      vTaskDelay(pdMS_TO_TICKS(500));
      continue;
    }

    // Connect MQTT
    if (!client.connected()) {

      sysState = DEGRADED;

      String cid =
        "ESP32Health-" +
        String((uint32_t)esp_random(), HEX);

      Serial.println("Connecting to MQTT broker...");

      if (client.connect(cid.c_str())) {

        Serial.println("MQTT connected!");

        bool subscribed = client.subscribe(topicControl);

        Serial.println(
          subscribed
            ? "Subscribed to control topics"
            : "Control topic subscription failed"
        );

        sysState = ONLINE;
      }
      else {

        Serial.printf(
          "MQTT connection failed, state=%d\n",
          client.state()
        );

        vTaskDelay(pdMS_TO_TICKS(3000));
        continue;
      }
    }

    // Process MQTT messages
    client.loop();

    uint32_t now = millis();

    if (now - lastPublish >= 2000) {

      lastPublish = now;

      VitalData v;
      EnvData e;

      bool haveVitals =
        vitalQueue &&
        xQueuePeek(vitalQueue, &v, 0) == pdTRUE;

      bool haveEnv =
        envQueue &&
        xQueuePeek(envQueue, &e, 0) == pdTRUE;

      // Publish vital data
      if (haveVitals) {

        char payload[160];

        snprintf(
          payload, sizeof(payload),
          "{\"hr\":%d,\"spo2\":%d,"
          "\"bodyTemp\":%.2f,\"dose\":%d}",
          v.hr, v.spo2, v.bodyT, v.dose
        );

        if (client.publish(topicVitals, payload)) {

          Serial.printf(
            "[MQTT VITALS] %s\n", payload
          );
        }
        else {
          Serial.println("Vitals publish failed");
        }
      }

      // Publish environmental data
      if (haveEnv) {

        char payload[120];

        snprintf(
          payload, sizeof(payload),
          "{\"temp\":%.2f,\"aqi\":%d,"
          "\"humidity\":%d}",
          e.roomT, e.aqi, e.humidity
        );

        if (!client.publish(topicEnv, payload)) {
          Serial.println("Environment publish failed");
        }
      }

      // Publish system status
      char statusPayload[120];

      snprintf(
        statusPayload, sizeof(statusPayload),
        "{\"bedAngle\":%d,\"targetBed\":%d,"
        "\"online\":1}",
        (int)currentBedAngle,
        (int)targetBedAngle
      );

      client.publish(topicStatus, statusPayload);
    }

    vTaskDelay(pdMS_TO_TICKS(20));
  }
}

// ---------- BED SERVO TASK ----------
void BedTask(void* pvParameters) {

  for (;;) {

    int requestedAngle =
      constrain((int)targetBedAngle, 0, 90);

    if (servoMutex &&
        xSemaphoreTake(
          servoMutex, pdMS_TO_TICKS(100)
        ) == pdTRUE) {

      if (currentBedAngle < requestedAngle) {
        currentBedAngle++;
      }
      else if (currentBedAngle > requestedAngle) {
        currentBedAngle--;
      }

      bedServo.write(currentBedAngle);

      xSemaphoreGive(servoMutex);
    }

    vTaskDelay(pdMS_TO_TICKS(100));
  }
}

// ---------- LCD TASK ----------
void LcdTask(void* pvParameters) {

  int page = 0;

  for (;;) {

    VitalData v;
    EnvData e;

    if (dataMutex &&
        xSemaphoreTake(
          dataMutex, pdMS_TO_TICKS(100)
        ) == pdTRUE) {

      v = g_lastVital;
      e = g_lastEnv;

      xSemaphoreGive(dataMutex);
    }
    else {
      v = g_lastVital;
      e = g_lastEnv;
    }

    if (lcdMutex &&
        xSemaphoreTake(
          lcdMutex, pdMS_TO_TICKS(100)
        ) == pdTRUE) {

      lcd.clear();

      if (page == 0) {

        lcd.setCursor(0, 0);
        lcd.print("HR:");
        lcd.print(v.hr);
        lcd.print(" S:");
        lcd.print(v.spo2);
        lcd.print("%");

        lcd.setCursor(0, 1);

        if (sysState == ONLINE) {
          lcd.print("ON ");
        }
        else if (sysState == DEGRADED) {
          lcd.print("MQTT ");
        }
        else {
          lcd.print("OFF ");
        }

        lcd.print("B:");
        lcd.print((int)currentBedAngle);
        lcd.print(" Q:");
        lcd.print((int)bufferIndex);
      }

      else if (page == 1) {

        lcd.setCursor(0, 0);
        lcd.print("Body:");
        lcd.print(v.bodyT, 1);
        lcd.print("C");

        lcd.setCursor(0, 1);
        lcd.print("Dose:");
        lcd.print(v.dose);
        lcd.print("% T:");
        lcd.print((int)targetDose);
      }

      else {

        lcd.setCursor(0, 0);
        lcd.print("Room:");
        lcd.print(e.roomT, 1);
        lcd.print("C");

        lcd.setCursor(0, 1);
        lcd.print("AQI:");
        lcd.print(e.aqi);
        lcd.print(" H:");
        lcd.print(e.humidity);
        lcd.print("%");
      }

      xSemaphoreGive(lcdMutex);
    }

    vTaskDelay(pdMS_TO_TICKS(2000));

    page = (page + 1) % 3;
  }
}

// ---------- SETUP ----------
void setup() {

  Serial.begin(115200);
  delay(500);

  Serial.println("\n=== Smart Hospital ESP32 ===");

  pinMode(LED_PIN, OUTPUT);
  pinMode(BUZZ_PIN, OUTPUT);

  digitalWrite(LED_PIN, LOW);
  digitalWrite(BUZZ_PIN, LOW);

  analogReadResolution(12);

  dht.begin();

  ds18b20.begin();
  ds18b20.setWaitForConversion(false);

  lcd.begin(16, 2);
  lcd.clear();
  lcd.print("Smart Hospital");

  bedServo.setPeriodHertz(50);
  bedServo.attach(SERVO_PIN, 500, 2400);
  bedServo.write(10);

  // WiFi is managed by MqttTask
  client.setServer(mqtt_server, mqtt_port);
  client.setCallback(callback);
  client.setKeepAlive(30);
  client.setSocketTimeout(5);
  client.setBufferSize(512);

  // FreeRTOS queues
  vitalQueue = xQueueCreate(1, sizeof(VitalData));
  envQueue = xQueueCreate(1, sizeof(EnvData));

  // Mutexes
  lcdMutex = xSemaphoreCreateMutex();
  servoMutex = xSemaphoreCreateMutex();
  dataMutex = xSemaphoreCreateMutex();

  if (!vitalQueue || !envQueue ||
      !lcdMutex || !servoMutex || !dataMutex) {

    Serial.println(
      "ERROR: FreeRTOS object creation failed!"
    );

    while (true) {
      delay(1000);
    }
  }

  // Initial values
  VitalData initialVital = {
    74, 98, 36.6f, 0
  };

  EnvData initialEnv = {
    24.0f, 0, 50
  };

  xQueueOverwrite(vitalQueue, &initialVital);
  xQueueOverwrite(envQueue, &initialEnv);

  BaseType_t result;

  result = xTaskCreate(
    SensorTask, "Sensor", 4096, nullptr, 1, nullptr
  );

  if (result != pdPASS) {
    Serial.println("ERROR: Sensor task creation failed!");
  }

  result = xTaskCreate(
    EnvTask, "Environment", 4096, nullptr, 1, nullptr
  );

  if (result != pdPASS) {
    Serial.println("ERROR: Environment task creation failed!");
  }

  result = xTaskCreate(
    MqttTask, "MQTT", 8192, nullptr, 2, nullptr
  );

  if (result != pdPASS) {
    Serial.println("ERROR: MQTT task creation failed!");
  }

  result = xTaskCreate(
    BedTask, "Bed", 4096, nullptr, 1, nullptr
  );

  if (result != pdPASS) {
    Serial.println("ERROR: Bed task creation failed!");
  }

  result = xTaskCreate(
    LcdTask, "LCD", 4096, nullptr, 1, nullptr
  );

  if (result != pdPASS) {
    Serial.println("ERROR: LCD task creation failed!");
  }

  Serial.println("Task initialization complete");
}

// ---------- LOOP ----------
void loop() {
  vTaskDelay(pdMS_TO_TICKS(1000));
}
