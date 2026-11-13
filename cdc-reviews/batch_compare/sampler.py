"""수집 프로세스와 컨테이너의 자원 사용량을 기록한다."""

import argparse
import json
import subprocess
import threading
import time

import psutil

ap = argparse.ArgumentParser()
ap.add_argument("--duration", type=int, required=True)
ap.add_argument("--out", default="batch_compare/logs/resources.jsonl")
args = ap.parse_args()
out = open(args.out, "w", encoding="utf-8")


def label(p):
    try:
        cmd = " ".join(p.cmdline())
    except (psutil.Error, OSError):
        return None
    if "consumer.py" in cmd:
        return "stream_consumer"
    if "batch_loader.py" in cmd and "--table" in cmd:
        return cmd.split("--table")[1].split()[0]
    return None


t_end = time.time() + args.duration
lock = threading.Lock()


def docker_loop():
    while time.time() < t_end:
        r = subprocess.run(
            [
                "docker",
                "stats",
                "--no-stream",
                "--format",
                "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}",
                "demo-postgres",
                "demo-kafka",
                "demo-connect",
            ],
            capture_output=True,
            text=True,
        )
        cont = {}
        for line in r.stdout.strip().splitlines():
            name, cpu, mem = line.split("|")
            used = mem.split("/")[0].strip()
            mb = (
                float(used[:-3]) * (1024 if used.endswith("GiB") else 1)
                if used[-3:] in ("GiB", "MiB")
                else 0.0
            )
            cont[name] = {"cpu_pct": float(cpu.strip("%")), "mem_mb": round(mb, 1)}
        with lock:
            out.write(json.dumps({"t": time.time(), "type": "docker", "cont": cont}) + "\n")
            out.flush()


threading.Thread(target=docker_loop, daemon=True).start()
while time.time() < t_end:
    now = time.time()
    procs = {}
    for p in psutil.process_iter(["pid"]):
        lab = label(p)
        if not lab:
            continue
        try:
            ct = p.cpu_times()
            rec = {"cpu_s": ct.user + ct.system, "rss_mb": p.memory_info().rss / 1e6}
        except psutil.Error:
            continue
        if lab not in procs or rec["rss_mb"] > procs[lab]["rss_mb"]:
            procs[lab] = rec
    with lock:
        out.write(json.dumps({"t": now, "type": "proc", "procs": procs}) + "\n")
    out.flush()
    time.sleep(max(0.0, 1.0 - (time.time() - now)))
