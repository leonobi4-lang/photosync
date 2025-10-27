#!/usr/bin/env python3
from flask import Flask, render_template, jsonify, request, Response
import subprocess
import threading
import os
import time
import json
from datetime import datetime

app = Flask(__name__, template_folder="templates", static_folder="static")

LOG_FILE = "/app/logs/sync.log"
HASH_FILE = "/app/cache/hash_cache.json"
SYNC_SCRIPT = "sync_photos_fast.py"

def run_sync(dry_run=False):
    env = os.environ.copy()
    env["DRY_RUN"] = "true" if dry_run else "false"
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"\n--- СИНХРОНИЗАЦИЯ: {datetime.now()} | DRY-RUN: {dry_run} ---\n")
    subprocess.Popen(["python", SYNC_SCRIPT], env=env)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/start", methods=["POST"])
def start():
    dry_run = request.json.get("dry_run", False)
    threading.Thread(target=run_sync, args=(dry_run,), daemon=True).start()
    return jsonify({"status": "started", "dry_run": dry_run})

@app.route("/logs")
def logs():
    def stream():
        if not os.path.exists(LOG_FILE):
            yield "Лог не найден. Запустите синхронизацию.\n"
            return
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            f.seek(0, 2)
            while True:
                line = f.readline()
                if not line:
                    time.sleep(0.1)
                    continue
                yield line
    return Response(stream(), mimetype="text/plain")

@app.route("/status")
def status():
    stats = {"files": 0}
    if os.path.exists(HASH_FILE):
        try:
            with open(HASH_FILE, "r") as f:
                cache = json.load(f)
                stats["files"] = len(cache)
        except: pass
    return jsonify(stats)

if __name__ == "__main__":

    app.run(host="0.0.0.0", port=5000, threaded=True)
