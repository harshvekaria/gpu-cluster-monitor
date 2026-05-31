import json
import time
import random
import os
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC             = os.getenv('KAFKA_TOPIC', 'gpu-telemetry')
NUM_GPUS                = int(os.getenv('NUM_GPUS', 12))
EMIT_INTERVAL_SEC       = 1.0

GPU_MODELS = [
    'NVIDIA A100 80GB', 'NVIDIA H100 80GB',
    'NVIDIA RTX 4090',  'NVIDIA A10G',
]
CLUSTER_NODES = [f'node-{i:02d}' for i in range(1, 5)]

class GPUState:
    def __init__(self, gpu_id):
        self.gpu_id    = gpu_id
        self.node      = random.choice(CLUSTER_NODES)
        self.model     = random.choice(GPU_MODELS)
        self.base_util = random.uniform(20, 85)
        self.temp      = random.uniform(45, 65)
        self.power     = random.uniform(100, 300)

    def next_reading(self):
        spike = random.random() < 0.05
        if spike:
            self.base_util = min(100, self.base_util + random.uniform(10, 30))
        else:
            self.base_util += random.uniform(-3, 3)
            self.base_util  = max(0, min(100, self.base_util))

        utilization = round(self.base_util + random.uniform(-2, 2), 2)
        utilization = max(0, min(100, utilization))

        target_temp  = 40 + (utilization / 100) * 50
        self.temp   += (target_temp - self.temp) * 0.1 + random.uniform(-1, 1)
        temperature  = round(self.temp, 2)

        target_power  = 80 + (utilization / 100) * 320
        self.power   += (target_power - self.power) * 0.1 + random.uniform(-5, 5)
        power_draw    = round(max(50, self.power), 2)

        mem_bandwidth = round(utilization * 7.8 + random.uniform(-20, 20), 2)
        mem_bandwidth = max(0, min(900, mem_bandwidth))

        mem_used_pct  = round(min(100, utilization * 0.85 + random.uniform(-5, 10)), 2)

        stress      = (temperature > 82) or (utilization > 95)
        error_code  = random.choice(['ECC_SINGLE', 'ECC_DOUBLE', 'THERMAL_WARNING',
                                     'POWER_LIMIT', 'XIDSTUCK']) if (stress and random.random() < 0.15) else 'NONE'

        sm_clock  = round(1200 + (utilization / 100) * 1200 + random.uniform(-50, 50))
        mem_clock = round(877 + random.uniform(-20, 20))

        return {
            'event_time'       : datetime.now(timezone.utc).isoformat(),
            'gpu_id'           : self.gpu_id,
            'node'             : self.node,
            'gpu_model'        : self.model,
            'utilization_pct'  : utilization,
            'temperature_c'    : temperature,
            'power_draw_w'     : power_draw,
            'mem_bandwidth_gbs': mem_bandwidth,
            'mem_used_pct'     : mem_used_pct,
            'sm_clock_mhz'     : sm_clock,
            'mem_clock_mhz'    : mem_clock,
            'error_code'       : error_code,
        }

def make_producer(retries=10, delay=5):
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
            )
            log.info('Connected to Kafka at %s', KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            log.warning('Kafka not ready - attempt %d/%d, retrying in %ds', attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError('Could not connect to Kafka after %d attempts' % retries)

def main():
    gpus     = [GPUState(gpu_id=f'gpu-{i:02d}') for i in range(NUM_GPUS)]
    producer = make_producer()

    log.info('Streaming telemetry for %d GPUs -> topic: %s', NUM_GPUS, KAFKA_TOPIC)
    while True:
        for gpu in gpus:
            reading = gpu.next_reading()
            producer.send(KAFKA_TOPIC, value=reading)
            log.info('Sent  gpu_id=%-8s util=%5.1f%%  temp=%5.1fC  power=%6.1fW  err=%s',
                     reading['gpu_id'], reading['utilization_pct'],
                     reading['temperature_c'], reading['power_draw_w'],
                     reading['error_code'])
        producer.flush()
        time.sleep(EMIT_INTERVAL_SEC)

if __name__ == '__main__':
    main()