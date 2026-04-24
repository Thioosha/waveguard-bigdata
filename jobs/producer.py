# =============================================================
# producer.py — WaveGuard : Simulateur de transactions Mobile Money
# Ecole Polytechnique de Thiès — Big Data DIC2/GIT 2025-2026
#
# USAGE : python producer.py
# (depuis la machine hôte, pas depuis Docker)
# BROKER cible : localhost:9092
# =============================================================

from confluent_kafka import Producer
from faker import Faker
import json, time, random, uuid
from datetime import datetime, timezone

fake = Faker('fr_FR')

# ── Config broker ─────────────────────────────────────────────
# Depuis l'hôte → localhost:9092
# Depuis le conteneur Spark → kafka:9093
BROKER = 'localhost:9092'
TOPIC  = 'transactions'

# ── Comptes ───────────────────────────────────────────────────
ACCOUNTS       = [f'SN_{i:04d}' for i in range(1, 51)]
FRAUD_ACCOUNTS = ['SN_0042', 'SN_0007', 'SN_0013']  # suspects

conf     = {'bootstrap.servers': BROKER}
producer = Producer(conf)

# ── Callback livraison ────────────────────────────────────────
def delivery_report(err, msg):
    if err:
        print(f'[ERREUR] Livraison échouée : {err}')
    else:
        print(
            f'[OK] sender={msg.key().decode():10s} | '
            f'partition={msg.partition()} | '
            f'offset={msg.offset()}'
        )

# ── Génération d'une transaction ──────────────────────────────
def generate_transaction(sender_id, fraud=False):
    """
    fraud=True  → montant élevé (attaque volume) + is_flagged=True
    fraud=False → montant normal
    La clé Kafka = sender_id → même partition garantie (ordre par compte)
    """
    amount = (
        random.randint(800_000, 2_000_000) if fraud
        else random.randint(500, 150_000)
    )
    return {
        'transaction_id':   str(uuid.uuid4()),
        'timestamp':        datetime.now(timezone.utc).isoformat(),
        'sender_id':        sender_id,
        'receiver_id':      random.choice(ACCOUNTS),
        'amount_fcfa':      amount,
        'transaction_type': random.choice(['P2P', 'PAIEMENT_MARCHAND', 'RETRAIT']),
        'location':         random.choice(['Dakar', 'Thiès', 'Saint-Louis',
                                           'Ziguinchor', 'Kaolack']),
        'is_flagged':       fraud,   # vérité terrain pour valider la détection
    }

# ── Envoi avec partitionnement explicite par sender_id ────────
def send_tx(tx):
    """
    La clé = sender_id assure que toutes les transactions
    d'un même compte arrivent dans la même partition Kafka.
    """
    producer.produce(
        TOPIC,
        key=tx['sender_id'],           # partitionnement par compte
        value=json.dumps(tx).encode(),
        callback=delivery_report
    )
    producer.poll(0)  # déclenche les callbacks en attente

# ── Burst frauduleux (attaque par vélocité) ───────────────────
def burst_fraud(sender_id):
    """
    Envoie 8 transactions en rafale espacées de 50ms.
    Simule une attaque par vélocité : >5 tx en <5 minutes.
    """
    print(f'\n[BURST] Attaque vélocité depuis {sender_id} — 8 tx en rafale')
    for i in range(8):
        tx = generate_transaction(sender_id, fraud=True)
        send_tx(tx)
        time.sleep(0.05)  # 50ms entre chaque tx
    print(f'[BURST] Rafale terminée pour {sender_id}\n')

# ── Boucle principale ─────────────────────────────────────────
print('=' * 55)
print(' WaveGuard Producer démarré')
print(f' Broker : {BROKER}  |  Topic : {TOPIC}')
print(' Ctrl+C pour arrêter')
print('=' * 55)

try:
    while True:
        is_fraud = random.random() < 0.10  # 10% de transactions frauduleuses

        if is_fraud:
            fraud_account = random.choice(FRAUD_ACCOUNTS)
            # 50% chance de burst (vélocité), 50% tx isolée suspecte (volume)
            if random.random() < 0.5:
                burst_fraud(fraud_account)
            else:
                tx = generate_transaction(fraud_account, fraud=True)
                send_tx(tx)
        else:
            sender = random.choice(ACCOUNTS)
            tx = generate_transaction(sender, fraud=False)
            send_tx(tx)

        time.sleep(random.uniform(0.05, 0.3))  # ~5 à 20 tx/sec

except KeyboardInterrupt:
    print('\nArrêt du producer...')
    producer.flush()
    print('Tous les messages ont été envoyés. Au revoir !')
