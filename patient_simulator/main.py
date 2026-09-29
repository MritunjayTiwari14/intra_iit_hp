import json
import time
import random
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("patient_simulator")

KAFKA_BROKER = "localhost:19092"
TOPIC = "vitals_stream"

# Baseline vitals for our 5 patients
PATIENTS = [
    {"patient_id": "pt_30045", "hr": 88, "sbp": 128, "dbp": 76, "spo2": 96, "rr": 16},
    {"patient_id": "pt_20419", "hr": 82, "sbp": 135, "dbp": 82, "spo2": 93, "rr": 18},
    {"patient_id": "pt_10882", "hr": 78, "sbp": 122, "dbp": 78, "spo2": 97, "rr": 15},
    {"patient_id": "pt_40291", "hr": 92, "sbp": 118, "dbp": 72, "spo2": 94, "rr": 20},
    {"patient_id": "pt_50163", "hr": 72, "sbp": 130, "dbp": 82, "spo2": 98, "rr": 14},
]

def get_base_vitals(patient_id):
    for pt in PATIENTS:
        if pt["patient_id"] == patient_id:
            return dict(pt)
    return None

def main():
    logger.info(f"Connecting to Kafka at {KAFKA_BROKER}...")
    producer = None
    
    # Retry loop in case Kafka is still spinning up
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except Exception as e:
            logger.warning(f"Waiting for Kafka broker at {KAFKA_BROKER}... retrying in 3s")
            time.sleep(3)

    logger.info("✅ Connected to Kafka! Starting dynamic randomized simulation...")

    current_state = {pt["patient_id"]: dict(pt) for pt in PATIENTS}
    active_spike = None # {"patient_id": "pt_xxx", "type": "sepsis", "ticks_remaining": 2}
    ticks_until_next_spike = 8 # Approx 15 seconds (8 ticks * 2s)

    try:
        while True:
            # 1. Handle Spike Lifecycle
            if active_spike:
                active_spike["ticks_remaining"] -= 1
                if active_spike["ticks_remaining"] <= 0:
                    logger.info(f"🟢 RECOVERY: {active_spike['patient_id']} stabilized and is returning to baseline.")
                    # Revert back to normal
                    current_state[active_spike["patient_id"]] = get_base_vitals(active_spike["patient_id"])
                    active_spike = None
                    ticks_until_next_spike = 8 # Approx 15 seconds between spikes

            if not active_spike:
                ticks_until_next_spike -= 1
                if ticks_until_next_spike <= 0:
                    # TRIGGER RANDOM SPIKE
                    target_pt = random.choice(PATIENTS)["patient_id"]
                    spike_type = random.choice(["sepsis", "hypoxia"])
                    active_spike = {"patient_id": target_pt, "type": spike_type, "ticks_remaining": 3} # Approx 5-6 seconds
                    
                    logger.warning(f"🚨 SPIKE TRIGGERED: {target_pt} is experiencing {spike_type.upper()}!")
                    
                    pt_state = current_state[target_pt]
                    if spike_type == "sepsis":
                        pt_state["hr"] = 125
                        pt_state["sbp"] = 85
                        pt_state["dbp"] = 55
                    elif spike_type == "hypoxia":
                        pt_state["spo2"] = 85
                        pt_state["rr"] = 28

            # 2. Generate and publish vitals
            for pid, state in current_state.items():
                vital_reading = {
                    "patient_id": pid,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "hr": state["hr"] + random.uniform(-2, 2),
                    "sbp": state["sbp"] + random.uniform(-3, 3),
                    "dbp": state["dbp"] + random.uniform(-2, 2),
                    "spo2": min(100, state["spo2"] + random.uniform(-1, 1)),
                    "rr": max(8, state["rr"] + random.uniform(-1, 1)),
                }

                producer.send(TOPIC, vital_reading)
                # Only log the spiking patient to reduce console spam, or log all if you prefer
                if active_spike and active_spike["patient_id"] == pid:
                    logger.warning(f"   ↳ 🔴 {pid} (SPIKING): HR={vital_reading['hr']:.0f}, BP={vital_reading['sbp']:.0f}/{vital_reading['dbp']:.0f}, SpO2={vital_reading['spo2']:.0f}%")
                else:
                    logger.info(f"   ↳ 🟢 {pid}: HR={vital_reading['hr']:.0f}, SpO2={vital_reading['spo2']:.0f}%")
            
            producer.flush()
            print("-" * 50)
            time.sleep(2)
            
    except KeyboardInterrupt:
        logger.info("\n🛑 Simulation stopped by user.")
    finally:
        if producer:
            producer.close()

if __name__ == "__main__":
    main()
