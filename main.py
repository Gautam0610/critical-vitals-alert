import json
import time
from kafka import KafkaProducer

# Existing anomaly detection logic (assuming it exists)
def is_anomaly(vital_value):
    # Replace with your actual anomaly detection logic
    return vital_value > 100  # Example: Vital value > 100 is considered an anomaly

# Configuration
KAFKA_BROKER = 'localhost:9092'  # Replace with your Kafka broker address
VITALS_TOPIC = 'vitals'
VITALS_HIGH_RISK_TOPIC = 'vitals_high_risk'

# Kafka producer
producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER])

# Track consecutive critical events
consecutive_critical_events = {}  # {vital_name: count}
CONSECUTIVE_THRESHOLD = 3  # Number of consecutive events to trigger high-risk alert

def process_vital(vital_name, vital_value):
    timestamp = time.time()
    is_critical = is_anomaly(vital_value)

    # Log event
    event = {
        'vital_name': vital_name,
        'vital_value': vital_value,
        'timestamp': timestamp,
        'is_critical': is_critical
    }
    log_message = json.dumps(event)
    print(log_message) # Log to console

    # Produce to Kafka topic (vitals)
    producer.send(VITALS_TOPIC, key=vital_name.encode('utf-8'), value=log_message.encode('utf-8'))

    # Check for consecutive critical events
    if is_critical:
        if vital_name in consecutive_critical_events:
            consecutive_critical_events[vital_name] += 1
        else:
            consecutive_critical_events[vital_name] = 1

        if consecutive_critical_events[vital_name] >= CONSECUTIVE_THRESHOLD:
            # Trigger high-risk alert
            high_risk_message = {
                'vital_name': vital_name,
                'message': f'HIGH RISK: {vital_name} has been critical for {CONSECUTIVE_THRESHOLD} consecutive readings.',
                'timestamp': timestamp
            }
            high_risk_message_json = json.dumps(high_risk_message)
            producer.send(VITALS_HIGH_RISK_TOPIC, key=vital_name.encode('utf-8'), value=high_risk_message_json.encode('utf-8'))
            # Reset counter after sending alert
            consecutive_critical_events[vital_name] = 0
    else:
        # Reset counter if not critical
        consecutive_critical_events[vital_name] = 0

# Example usage (replace with your actual data source)
if __name__ == '__main__':
    # Simulate vital readings
    vitals_data = [
        ('heart_rate', 70),
        ('heart_rate', 110),
        ('heart_rate', 120),
        ('heart_rate', 130),
        ('heart_rate', 75),
        ('blood_pressure', 80),
        ('blood_pressure', 150),
        ('blood_pressure', 160),
        ('blood_pressure', 170),
        ('blood_pressure', 90),
    ]

    for vital_name, vital_value in vitals_data:
        process_vital(vital_name, vital_value)

    producer.flush() # Ensure all messages are sent
