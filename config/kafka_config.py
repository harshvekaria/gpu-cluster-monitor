import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC             = os.getenv('KAFKA_TOPIC', 'gpu-telemetry')
KAFKA_GROUP_ID          = 'gpu-monitor-group'

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
POSTGRES_USER = os.getenv('POSTGRES_USER', 'gpuadmin')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'gpupass123')
POSTGRES_DB   = os.getenv('POSTGRES_DB', 'gpu_monitor')
