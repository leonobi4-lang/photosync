#!/usr/bin/env python3
import os
import json
import xxhash
import shutil
import time
import logging
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import RotatingFileHandler

HASH_FILE = "/app/hash_cache.json"
LOG_FILE = "/logs/sync.log"

ALGO = os.getenv("HASH_ALGO", "xxh64")
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
MODE = os.getenv("MODE", "move").lower()
STRUCTURE = os.getenv("STRUCTURE", "true").lower() == "true"
IGNORE_DIRS = [x.strip().lower() for x in os.getenv("IGNORE_DIRS", "@eaDir,tmp,cache").split(",")]
THREADS = int(os.getenv("THREADS", "4"))

SRC = os.getenv("SRC", "/duplicates")
DST = os.getenv("DST", "/sorted")

MIN_SIZE = 30 * 1024
EXCLUDE_EXT = {".tmp", ".db", ".ini", ".aae", ".json", ".txt", ".log"}
EXCLUDE_NAMES = {"thumbs.db", ".nomedia"}

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    os.makedirs("/logs", exist_ok=True)
    fh = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3)
    fh.setFormatter(logging.Formatter("[%(asctime)s] %(message)s"))
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", "%H:%M:%S"))
    logger.addHandler(ch)

def log(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line, flush=True)
    logging.info(msg)

def get_hasher():
    if ALGO.startswith("xxh"):
        return getattr(xxhash, ALGO)() if hasattr(xxhash, ALGO) else xxhash.xxh64()
    import hashlib
    return hashlib.new(ALGO)

def file_hash(path, cache):
    try:
        stat = os.stat(path)
        size = stat.st_size
        mtime = int(stat.st_mtime)
        if size < MIN_SIZE: return None
        name = os.path.basename(path).lower()
        if name in EXCLUDE_NAMES or any(path.lower().endswith(e) for e in EXCLUDE_EXT): return None
        if path in cache and cache[path]["size"] == size and cache[path]["mtime"] == mtime:
            return cache[path]["hash"]
        h = get_hasher()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        digest = h.hexdigest() if hasattr(h, "hexdigest") else h.hex()
        cache[path] = {"size": size, "mtime": mtime, "hash": digest}
        return digest
    except Exception as e:
        log(f"Ошибка: {path} — {e}")
        return None

def load_cache():
    if not os.path.exists(HASH_FILE): return {}
    try:
        with open(HASH_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        return {p: c for p, c in cache.items() if os.path.exists(p)}
    except: return {}

def save_cache(cache):
    try:
        with open(HASH_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False)
    except: pass

def collect_hashes_parallel(base, cache):
    if not os.path.exists(base): return {}
    files = [os.path.join(r, f) for r, _, fs in os.walk(base) for f in fs]
    log(f"Хэшируем {len(files)} файлов из {base}...")
    hashes = {}
    with ThreadPoolExecutor(max_workers=THREADS) as e:
        futures = {e.submit(file_hash, p, cache): p for p in files}
        for f in tqdm(as_completed(futures), total=len(futures), desc="Хэш", leave=False):
            h = f.result()
            if h and h not in hashes:
                hashes[h] = futures[f]
    return hashes

def safe_move(src, dst):
    try:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        if os.path.exists(dst):
            base, ext = os.path.splitext(dst)
            i = 1
            while os.path.exists(f"{base}_{i}{ext}"):
                i += 1
            dst = f"{base}_{i}{ext}"
        if DRY_RUN:
            log(f"[DRY] {src} → {dst}")
        else:
            if MODE == "move":
                shutil.move(src, dst)
            else:
                shutil.copy2(src, dst)
            log(f"{src} → {dst}")
        return True
    except Exception as e:
        log(f"Ошибка: {e}")
        return False

def remove_empty_dirs(root):
    for p in sorted(Path(root).rglob("*"), key=lambda x: len(x.parts), reverse=True):
        if p.is_dir() and not any(p.iterdir()):
            try: p.rmdir()
            except: pass

def main():
    setup_logging()
    log("Запуск синхронизации...")
    t0 = time.time()

    cache = load_cache()
    dst_hashes = collect_hashes_parallel(DST, cache)
    src_hashes = collect_hashes_parallel(SRC, cache)

    new_files = [p for h, p in src_hashes.items() if h not in dst_hashes]
    log(f"Найдено {len(new_files)} новых файлов")

    moved = 0
    for path in new_files:
        if STRUCTURE:
            dt = datetime.fromtimestamp(os.path.getmtime(path))
            subdir = f"{dt.year}/{dt.month:02d}"
            dest = os.path.join(DST, subdir, os.path.basename(path))
        else:
            rel = os.path.relpath(path, SRC)
            dest = os.path.join(DST, rel)
        if safe_move(path, dest):
            moved += 1

    save_cache(cache)
    if not DRY_RUN:
        remove_empty_dirs(SRC)

    log(f"Готово! {moved}/{len(new_files)} за {time.time()-t0:.1f}с")

if __name__ == "__main__":
    main()