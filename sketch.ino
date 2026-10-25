
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
#define MQ2_PIN    39   // VN / GPIO39 (ADC1)
#define DOSE_PIN   36   // VP / GPIO36 (ADC1)
#define SERVO_PIN  18
#define LED_PIN    23
#define BUZZ_PIN   2

#define DHTTYPE DHT22

// ---------- SENSORS ----------
DHT dht(DHT_PIN, DHTTYPE);
OneWire oneWire(DS18_PIN);
DallasTemperature ds18b20(&oneWire);
Servo bedServo;

// ---------- WIFI & MQTT ----------
const char* ssid = "Wokwi-GUEST";
const char* password = "";
const char* mqtt_server = "broker.emqx.io";
const int mqtt_port = 1883;

WiFiClient espClient;
PubSubClient client(espClient);

// ---------- FREERTOS OBJECTS ----------
QueueHandle_t vitalQueue = NULL;
QueueHandle_t envQueue = NULL;
SemaphoreHandle_t lcdMutex = NULL;
SemaphoreHandle_t servoMutex = NULL;

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
};

#define BUFFER_SIZE 20

VitalData offlineBuffer[BUFFER_SIZE];
volatile int bufferIndex = 0;

volatile int samplingRate = 5;
volatile int targetBedAngle = 10;
volatile int currentBedAngle = 10;
volatile int lastDose = 0;
volatile int targetDose = 0;

VitalData g_lastVital = {74, 90, 36.6, 0};
EnvData g_lastEnv = {24.0, 0};

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
  } else if (dose > 50) {
    digitalWrite(LED_PIN, HIGH);
    digitalWrite(BUZZ_PIN, LOW);
  } else {
    digitalWrite(LED_PIN, LOW);
    digitalWrite(BUZZ_PIN, LOW);
  }
}

// ---------- MQTT CALLBACK ----------
void callback(char* topic, byte* payload, unsigned int length) {
  String msg;
  msg.reserve(length);

  for (unsigned int i = 0; i < length; i++) {
    msg += (char)payload[i];
  }

  String t(topic);

  Serial.print("CONTROL: ");
  Serial.print(t);
  Serial.print(" -> ");
  Serial.println(msg);

  if (t.indexOf("bed") >= 0) {
    if (msg == "sleep") {
      targetBedAngle = 10;
    } else if (msg == "breath") {
      targetBedAngle = 45;
    } else if (msg == "emergency") {
      targetBedAngle = 90;
    } else {
      targetBedAngle = constrain(msg.toInt(), 0, 90);
    }
  }

  if (t.indexOf("dosage") >= 0) {
    targetDose = constrain(msg.toInt(), 0, 100);
    Serial.printf("Target dose: %d\n", (int)targetDose);
  }

  if (t.indexOf("sampling") >= 0) {
    samplingRate = constrain(msg.toInt(), 5, 60);
  }
}

// ---------- SENSOR TASK ----------
void SensorTask(void* pvParameters) {
  VitalData v;

  for (;;) {
    v.hr = map(analogRead(LDR_PIN), 0, 4095, 60, 120);
    v.spo2 = map(analogRead(SPO2_POT), 0, 4095, 90, 100);

    // Start DS18B20 conversion without blocking the CPU
    ds18b20.requestTemperatures();
    vTaskDelay(pdMS_TO_TICKS(750));

    v.bodyT = ds18b20.getTempCByIndex(0);

    if (v.bodyT == DEVICE_DISCONNECTED_C ||
        isnan(v.bodyT) || v.bodyT < 10 || v.bodyT > 60) {
      v.bodyT = 36.6;
    }

    int rawDose = analogRead(DOSE_PIN);
    int measuredDose = map(rawDose, 0, 4095, 0, 100);

    if (abs(measuredDose - (int)lastDose) <= 20) {
      lastDose = measuredDose;
    } else {
      measuredDose = lastDose;
    }

    v.dose = measuredDose;

    // Retain target-dose simulation behaviour
    if (rawDose < 50 && targetDose > 0) {
      v.dose = targetDose;
    }

    g_lastVital = v;

    Serial.printf(
      "[SENSOR] HR:%d SpO2:%d BodyT:%.1f Dose:%d\n",
      v.hr, v.spo2, v.bodyT, v.dose
    );

    dosageSafetyCheck(targetDose > 0 ? targetDose : v.dose);

    // Queue length is exactly 1
    if (xQueueOverwrite(vitalQueue, &v) != pdPASS) {
      Serial.println("Vital queue write failed");
    }

    int delaySeconds = samplingRate;

    if (v.hr > 100 || v.spo2 < 93 || v.bodyT > 38) {
      delaySeconds = 5;
    }

    vTaskDelay(pdMS_TO_TICKS(delaySeconds * 1000));
  }
}

// ---------- ENVIRONMENT TASK ----------
void EnvTask(void* pvParameters) {
  EnvData e;

  for (;;) {
    e.roomT = dht.readTemperature();

    if (isnan(e.roomT)) {
      e.roomT = 24.0;
    }

    e.aqi = map(analogRead(MQ2_PIN), 0, 4095, 0, 300);
    g_lastEnv = e;

    if (xQueueOverwrite(envQueue, &e) != pdPASS) {
      Serial.println("Environment queue write failed");
    }

    Serial.printf("[ENV] Room:%.1f AQI:%d\n", e.roomT, e.aqi);

    vTaskDelay(pdMS_TO_TICKS(5000));
  }
}

// ---------- WIFI CONNECTION ----------
void connectWiFi() {
  if (WiFi.status() == WL_CONNECTED) {
    return;
  }

  Serial.println("Connecting to WiFi...");
  WiFi.disconnect();
  WiFi.begin(ssid, password);

  // Do not wait indefinitely for WiFi
  for (int i = 0; i < 10; i++) {
    if (WiFi.status() == WL_CONNECTED) {
      Serial.println("WiFi connected!");
      return;
    }

    vTaskDelay(pdMS_TO_TICKS(500));
  }

  Serial.println("WiFi not connected; retrying later");
}

// ---------- MQTT TASK ----------
void MqttTask(void* pvParameters) {
  connectWiFi();

  for (;;) {
    if (WiFi.status() != WL_CONNECTED) {
      sysState = OFFLINE;
      connectWiFi();
      vTaskDelay(pdMS_TO_TICKS(1000));
      continue;
    }

    if (!client.connected()) {
      sysState = DEGRADED;

      VitalData v;

      if (xQueuePeek(vitalQueue, &v, 0) == pdTRUE &&
          bufferIndex < BUFFER_SIZE) {
        offlineBuffer[bufferIndex++] = v;
      }

      String cid = "ESP32Health-" +
                   String((uint32_t)esp_random(), HEX);

      Serial.println("Connecting to MQTT...");

      if (client.connect(cid.c_str())) {
        Serial.println("MQTT connected!");

        client.subscribe("hospital/harini_final/control/#");

        // Clear the buffer only after reconnect.
        // Actual historical data replay is not implemented here.
        if (bufferIndex > 0) {
          Serial.printf("Buffered readings: %d\n", (int)bufferIndex);
          bufferIndex = 0;
        }
      } else {
        Serial.printf("MQTT failed, state=%d\n", client.state());
        vTaskDelay(pdMS_TO_TICKS(3000));
        continue;
      }
    }

    sysState = ONLINE;

    VitalData v;
    EnvData e;

    if (xQueuePeek(vitalQueue, &v, 0) == pdTRUE) {
      char payload[150];

      snprintf(
        payload, sizeof(payload),
        "{\"hr\":%d,\"spo2\":%d,\"bodyTemp\":%.2f,\"dose\":%d}",
        v.hr, v.spo2, v.bodyT, v.dose
      );

      if (client.publish("hospital/harini_final/vitals", payload)) {
        Serial.printf("VITALS: %s\n", payload);
      }
    }

    if (xQueuePeek(envQueue, &e, 0) == pdTRUE) {
      char payload[100];

      snprintf(
        payload, sizeof(payload),
        "{\"temp\":%.2f,\"aqi\":%d}",
        e.roomT, e.aqi
      );

      client.publish("hospital/harini_final/env", payload);
    }

    char statusPayload[100];

    snprintf(
      statusPayload, sizeof(statusPayload),
      "{\"bedAngle\":%d,\"targetBed\":%d,\"online\":1}",
      (int)currentBedAngle, (int)targetBedAngle
    );

    client.publish("hospital/harini_final/status", statusPayload);

    client.loop();
    vTaskDelay(pdMS_TO_TICKS(2000));
  }
}

// ---------- BED SERVO TASK ----------
void BedTask(void* pvParameters) {
  for (;;) {
    int requestedAngle = targetBedAngle;

    // Automatic positioning based on simulated readings
    if (g_lastVital.spo2 < 93) {
      requestedAngle = 45;
    }

    if (g_lastVital.hr > 110) {
      requestedAngle = 90;
    }

    requestedAngle = constrain(requestedAngle, 0, 90);

    if (xSemaphoreTake(servoMutex, portMAX_DELAY) == pdTRUE) {
      if (currentBedAngle < requestedAngle) {
        currentBedAngle++;
      } else if (currentBedAngle > requestedAngle) {
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
    if (xSemaphoreTake(lcdMutex, portMAX_DELAY) == pdTRUE) {
      lcd.clear();

      if (page == 0) {
        lcd.setCursor(0, 0);
        lcd.print("HR:");
        lcd.print(g_lastVital.hr);
        lcd.print(" S:");
        lcd.print(g_lastVital.spo2);
        lcd.print("%");

        lcd.setCursor(0, 1);

        if (sysState == ONLINE) {
          lcd.print("ON ");
        } else if (sysState == DEGRADED) {
          lcd.print("DEG ");
        } else {
          lcd.print("OFF ");
        }

        lcd.print("B:");
        lcd.print((int)currentBedAngle);
        lcd.print(" Q:");
        lcd.print((int)bufferIndex);

      } else if (page == 1) {
        lcd.setCursor(0, 0);
        lcd.print("Body:");
        lcd.print(g_lastVital.bodyT, 1);
        lcd.print("C");

        lcd.setCursor(0, 1);
        lcd.print("Dose:");
        lcd.print(g_lastVital.dose);
        lcd.print("% T:");
        lcd.print((int)targetDose);

      } else {
        lcd.setCursor(0, 0);
        lcd.print("Room:");
        lcd.print(g_lastEnv.roomT, 1);
        lcd.print("C");

        lcd.setCursor(0, 1);
        lcd.print("AQI:");
        lcd.print(g_lastEnv.aqi);
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
  lcd.print("Smart Hospital");

  bedServo.attach(SERVO_PIN);
  bedServo.write(10);

  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);

  client.setServer(mqtt_server, mqtt_port);
  client.setCallback(callback);
  client.setKeepAlive(60);
  client.setBufferSize(512);

  // Overwrite queues must have length 1
  vitalQueue = xQueueCreate(1, sizeof(VitalData));
  envQueue = xQueueCreate(1, sizeof(EnvData));

  lcdMutex = xSemaphoreCreateMutex();
  servoMutex = xSemaphoreCreateMutex();

  if (vitalQueue == NULL || envQueue == NULL ||
      lcdMutex == NULL || servoMutex == NULL) {
    Serial.println("ERROR: RTOS object creation failed!");

    while (true) {
      vTaskDelay(pdMS_TO_TICKS(1000));
    }
  }

  BaseType_t result;

  result = xTaskCreate(
    SensorTask, "Sensor", 4096, NULL, 1, NULL
  );
  if (result != pdPASS) {
    Serial.println("ERROR: Sensor task creation failed!");
  }

  result = xTaskCreate(
    EnvTask, "Environment", 4096, NULL, 1, NULL
  );
  if (result != pdPASS) {
    Serial.println("ERROR: Environment task creation failed!");
  }

  result = xTaskCreate(
    MqttTask, "MQTT", 8192, NULL, 2, NULL
  );
  if (result != pdPASS) {
    Serial.println("ERROR: MQTT task creation failed!");
  }

  result = xTaskCreate(
    BedTask, "Bed", 4096, NULL, 1, NULL
  );
  if (result != pdPASS) {
    Serial.println("ERROR: Bed task creation failed!");
  }

  result = xTaskCreate(
    LcdTask, "LCD", 4096, NULL, 1, NULL
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
