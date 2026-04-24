# =============================================================
# metrics_exporter.py — WaveGuard : Export métriques Grafana
# Ecole Polytechnique de Thiès — Big Data DIC2/GIT 2025-2026
#
# USAGE : python metrics_exporter.py
# (depuis la machine hôte, en parallèle du detector)
#
# Expose un serveur HTTP sur le port 9999 :
#   http://localhost:9999  → JSON des métriques WaveGuard
# Grafana Data Source : JSON API → http://host.docker.internal:9999
# =============================================================

import time
import json
import os
import glob
from http.server import HTTPServer, BaseHTTPRequestHandler

# ── Config ────────────────────────────────────────────────────
METRICS_FILE  = '/tmp/waveguard_metrics.json'
REFRESH_SEC   = 15    # actualisation toutes les 15 secondes
HTTP_PORT     = 9999  # port d'écoute pour Grafana

LAKE_VELOCITY = 'C:/tmp/waveguard_lake/velocity'
LAKE_VOLUME   = 'C:/tmp/waveguard_lake/volume'

# ── Calcul des métriques (via pandas, sans Spark) ─────────────
def compute_metrics():
    """Lit les fichiers Parquet du Data Lake et calcule les métriques."""
    metrics = {
        'timestamp':       time.time(),
        'velocity_alerts': 0,
        'volume_alerts':   0,
        'top_fraudster':   'N/A',
        'total_alerts':    0,
    }

    try:
        import pandas as pd

        # Alertes vélocité
        parquet_files = [f for f in glob.glob(os.path.join(LAKE_VELOCITY, '*.parquet')) if os.path.getsize(f) > 0]
        if parquet_files:
            frames = [pd.read_parquet(f) for f in parquet_files]
            df_v = pd.concat(frames, ignore_index=True)
            v_count = len(df_v)
            metrics['velocity_alerts'] = v_count

            if v_count > 0 and 'sender_id' in df_v.columns:
                top = df_v.groupby('sender_id').size().idxmax()
                metrics['top_fraudster'] = top
    except Exception as e:
        print(f'[WARN] Vélocité lake non disponible : {e}')

    try:
        import pandas as pd

        # Alertes volume
        parquet_files = [f for f in glob.glob(os.path.join(LAKE_VOLUME, '*.parquet')) if os.path.getsize(f) > 0]
        if parquet_files:
            frames = [pd.read_parquet(f) for f in parquet_files]
            df_vol = pd.concat(frames, ignore_index=True)
            metrics['volume_alerts'] = len(df_vol)
    except Exception as e:
        print(f'[WARN] Volume lake non disponible : {e}')

    metrics['total_alerts'] = (
        metrics['velocity_alerts'] + metrics['volume_alerts']
    )
    return metrics

# ── Serveur HTTP pour Grafana ─────────────────────────────────
class MetricsHandler(BaseHTTPRequestHandler):
    """Expose les métriques en JSON sur GET /"""

    def do_GET(self):
        try:
            with open(METRICS_FILE) as f:
                data = f.read()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(data.encode())
        except FileNotFoundError:
            self.send_response(503)
            self.end_headers()
            self.wfile.write(b'{"error": "metrics not yet available"}')

    def log_message(self, format, *args):
        pass  # silence les logs HTTP


def run_http_server():
    """Lance le serveur HTTP dans un thread séparé."""
    import threading
    server = HTTPServer(('0.0.0.0', HTTP_PORT), MetricsHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    print(f'[HTTP] Serveur métriques sur http://localhost:{HTTP_PORT}')
    return server

# ── Boucle principale ─────────────────────────────────────────
print('=' * 55)
print(' WaveGuard Metrics Exporter')
print(f' Rafraîchissement : toutes les {REFRESH_SEC}s')
print(f' Grafana URL : http://host.docker.internal:{HTTP_PORT}')
print('=' * 55)

run_http_server()

while True:
    try:
        metrics = compute_metrics()

        with open(METRICS_FILE, 'w') as f:
            json.dump(metrics, f, indent=2)

        print(
            f'[{time.strftime("%H:%M:%S")}] '
            f'velocity={metrics["velocity_alerts"]} | '
            f'volume={metrics["volume_alerts"]} | '
            f'top={metrics["top_fraudster"]}'
        )
    except Exception as e:
        print(f'[ERREUR] {e}')

    time.sleep(REFRESH_SEC)