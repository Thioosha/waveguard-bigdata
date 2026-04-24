# =============================================================
# waveguard_detector.py — WaveGuard : Moteur de détection Spark
# Ecole Polytechnique de Thiès — Big Data DIC2/GIT 2025-2026
#
# USAGE (depuis ta machine Windows, Spark installé localement) :
#   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 jobs/waveguard_detector.py
#
# Kafka tourne dans Docker → localhost:9092
# Spark tourne en local sur ta machine → local[*]
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window,
    count, sum as spark_sum,
    current_timestamp, lit,
    to_json, struct
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
    TimestampType, BooleanType
)

# ── Chemins locaux Windows ────────────────────────────────────
CHECKPOINT_BASE = 'C:/tmp/waveguard_checkpoint'
LAKE_BASE       = 'C:/tmp/waveguard_lake'

# ── Configuration Spark ───────────────────────────────────────
spark = SparkSession.builder \
    .appName('WaveGuard_FraudDetector') \
    .master('local[*]') \
    .config(
        'spark.jars.packages',
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0'
    ) \
    .config('spark.sql.streaming.checkpointLocation', CHECKPOINT_BASE) \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

print('=' * 55)
print(f' WaveGuard Detector — Spark {spark.version}')
print(f' Checkpoint : {CHECKPOINT_BASE}')
print(f' Data Lake  : {LAKE_BASE}')
print('=' * 55)

# ── Schéma JSON des transactions ──────────────────────────────
schema = StructType([
    StructField('transaction_id',   StringType(),    True),
    StructField('timestamp',        TimestampType(), True),
    StructField('sender_id',        StringType(),    True),
    StructField('receiver_id',      StringType(),    True),
    StructField('amount_fcfa',      DoubleType(),    True),
    StructField('transaction_type', StringType(),    True),
    StructField('location',         StringType(),    True),
    StructField('is_flagged',       BooleanType(),   True),
])

# ── Lecture du flux Kafka ─────────────────────────────────────
# Kafka tourne dans Docker, accessible depuis l'hôte sur localhost:9092
raw_stream = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'transactions') \
    .option('startingOffsets', 'earliest') \
    .option('failOnDataLoss', 'false') \
    .load()

# ── Parsing JSON + extraction des champs ──────────────────────
parsed_df = raw_stream \
    .selectExpr('CAST(value AS STRING) as json_str', 'timestamp as kafka_ts') \
    .select(from_json(col('json_str'), schema).alias('data'), col('kafka_ts')) \
    .select('data.*', 'kafka_ts')

# ── Watermark : tolérance aux messages en retard (2 min) ──────
df_with_watermark = parsed_df.withWatermark('timestamp', '2 minutes')

# =============================================================
# RÈGLE 1 — Fraude par vélocité
# Détection : > 5 transactions depuis un même compte en 5 min
# Fenêtre glissante : 5 min / slide 1 min
# =============================================================
velocity_fraud = df_with_watermark \
    .groupBy(
        window(col('timestamp'), '5 minutes', '1 minute'),
        col('sender_id')
    ) \
    .agg(count('*').alias('tx_count')) \
    .filter(col('tx_count') > 5) \
    .select(
        col('sender_id'),
        col('window.start').alias('window_start'),
        col('window.end').alias('window_end'),
        col('tx_count'),
        lit('VELOCITY_FRAUD').alias('fraud_type'),
        current_timestamp().alias('detected_at')
    )

# =============================================================
# RÈGLE 2 — Fraude par volume
# Détection : montant total > 500 000 FCFA depuis un compte en 10 min
# Fenêtre glissante : 10 min / slide 2 min
# =============================================================
volume_fraud = df_with_watermark \
    .groupBy(
        window(col('timestamp'), '10 minutes', '2 minutes'),
        col('sender_id')
    ) \
    .agg(spark_sum('amount_fcfa').alias('total_amount')) \
    .filter(col('total_amount') > 500_000) \
    .select(
        col('sender_id'),
        col('window.start').alias('window_start'),
        col('window.end').alias('window_end'),
        col('total_amount'),
        lit('VOLUME_FRAUD').alias('fraud_type'),
        current_timestamp().alias('detected_at')
    )

# =============================================================
# SINKS — Écriture des alertes
# =============================================================

def write_to_kafka(df, label):
    """
    Sink Kafka → topic fraud-alerts
    outputMode=update : envoie uniquement les lignes nouvelles/modifiées.
    """
    return df \
        .select(to_json(struct('*')).alias('value')) \
        .writeStream \
        .queryName(f'kafka_{label}') \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('topic', 'fraud-alerts') \
        .option('checkpointLocation', f'{CHECKPOINT_BASE}/kafka_{label}') \
        .outputMode('update') \
        .start()


def write_to_datalake(df, label):
    """
    Sink Parquet → Data Lake local (C:/tmp/waveguard_lake/)
    outputMode=append : écrit seulement les lignes finalisées après watermark.
    """
    return df \
        .writeStream \
        .queryName(f'lake_{label}') \
        .format('parquet') \
        .option('path', f'{LAKE_BASE}/{label}') \
        .option('checkpointLocation', f'{CHECKPOINT_BASE}/lake_{label}') \
        .outputMode('append') \
        .trigger(processingTime='30 seconds') \
        .start()


# ── Lancement des 4 queries ───────────────────────────────────
print('[DÉMARRAGE] Règle 1 — Vélocité → Kafka')
q1_kafka = write_to_kafka(velocity_fraud, 'velocity')

print('[DÉMARRAGE] Règle 1 — Vélocité → Parquet (Data Lake)')
q1_lake = write_to_datalake(velocity_fraud, 'velocity')

print('[DÉMARRAGE] Règle 2 — Volume → Kafka')
q2_kafka = write_to_kafka(volume_fraud, 'volume')

print('[DÉMARRAGE] Règle 2 — Volume → Parquet (Data Lake)')
q2_lake = write_to_datalake(volume_fraud, 'volume')

print('\n[OK] 4 queries actives. Ctrl+C pour arrêter.')
print('     Spark UI : http://localhost:4040\n')

# Attendre indéfiniment
spark.streams.awaitAnyTermination()