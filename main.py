import time
import random
import json
from kafka import KafkaProducer
from faker import Faker
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka configuration
KAFKA_TOPIC = "vitals-alerts"
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']  # Replace with your Kafka brokers

# Vitals thresholds
HEART_RATE_LOW = 50
HEART_RATE_HIGH = 120
অক্সিজেন_স্যাচুরেশন_LOW = 90
BLOOD_PRESSURE_LOW = 90
BLOOD_PRESSURE_HIGH = 140

# Faker for realistic data
fake = Faker()

def generate_vitals(patient_id):
    """Generates random vital signs for a patient."""
    heart_rate = random.randint(40, 130)
    oxygen_saturation = random.randint(85, 100)
    blood_pressure = random.randint(80, 150)
    temperature = round(random.uniform(36.0, 40.0), 1)

    return {
        'patient_id': patient_id,
        'timestamp': time.time(),
        'heart_rate': heart_rate,
        'oxygen_saturation': oxygen_saturation,
        'blood_pressure': blood_pressure,
        'temperature': temperature
    }

def check_critical_vitals(vitals):
    """Checks if vital signs are below critical thresholds."""
    alerts = []
    if vitals['heart_rate'] < HEART_RATE_LOW:
        alerts.append(f"Low heart rate: {vitals['heart_rate']}")
    if vitals['heart_rate'] > HEART_RATE_HIGH:
        alerts.append(f"High heart rate: {vitals['heart_rate']}")
    if vitals['oxygen_saturation'] < অক্সিজেন_স্যাচুরেশন_LOW:
        alerts.append(f"Low oxygen saturation: {vitals['oxygen_saturation']}")
    if vitals['blood_pressure'] < BLOOD_PRESSURE_LOW:
        alerts.append(f"Low blood pressure: {vitals['blood_pressure']}")
    if vitals['blood_pressure'] > BLOOD_PRESSURE_HIGH:
        alerts.append(f"High blood pressure: {vitals['blood_pressure']}")
    return alerts

def send_to_kafka(producer, topic, message):
    """Sends a message to a Kafka topic."""
    try:
        producer.send(topic, json.dumps(message).encode('utf-8'))
        logging.info(f"Sent message to Kafka: {message}")
    except Exception as e:
        logging.error(f"Error sending to Kafka: {e}")

def main():
    """Main function to generate vitals, check for critical conditions, and send alerts."""
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    patient_ids = [fake.uuid4() for _ in range(5)]  # Generate 5 patient IDs

    while True:
        for patient_id in patient_ids:
            vitals = generate_vitals(patient_id)
            logging.info(f"Vitals for patient {patient_id}: {vitals}")

            alerts = check_critical_vitals(vitals)
            if alerts:
                alert_message = {
                    'patient_id': patient_id,
                    'vitals': vitals,
                    'alerts': alerts
                }
                send_to_kafka(producer, KAFKA_TOPIC, alert_message)

        time.sleep(5)  # Generate vitals every 5 seconds

if __name__ == "__main__":
    main()