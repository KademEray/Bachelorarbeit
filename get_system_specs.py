"""
get_system_specs.py
────────────────────────────────────────────────────────────
Sammelt die wesentlichen Hardware- und Runtime-Daten für
Benchmark-Dokumentationen (Linux, macOS, Windows).

Erfasst:
  • CPU-Modell, phys./log. Kerne, max. Takt
  • RAM-Gesamtkapazität
  • Betriebssystem & Kernel/Build
  • Wichtigste Datenträger (Größe & mutmaßlicher Typ)
  • Docker- und Docker-Compose-Version (falls installiert)

Ausgabe:
  • menschenlesbare Markdown-Tabelle
  • optional JSON (Parameter --json)

❯  python get_system_specs.py          # Markdown auf STDOUT
❯  python get_system_specs.py --json   # JSON auf STDOUT
"""

from __future__ import annotations
import platform, json, shutil, subprocess, re, sys
from pathlib import Path

try:
    import psutil           # komfortablere RAM/CPU-Infos
except ImportError:
    psutil = None           # Skript funktioniert auch ohne

# ─────────────────────────── Helper ──────────────────────────────
def _run(cmd: list[str]) -> str:
    try:
        return subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return ""

def _detect_cpu() -> dict:
    plat = platform.system()
    if psutil:
        freq = psutil.cpu_freq()
        max_mhz = round(freq.max, 1) if freq else None
    else:
        max_mhz = None

    if plat == "Windows":
        name = _run(["wmic", "cpu", "get", "name"]).splitlines()[1:]
        model = " ".join(t.strip() for t in name if t.strip())
    elif plat == "Darwin":
        model = _run(["sysctl", "-n", "machdep.cpu.brand_string"])
    else:  # Linux & andere UNIX
        with open("/proc/cpuinfo", encoding="utf-8") as f:
            for line in f:
                if "model name" in line:
                    model = line.split(":", 1)[1].strip()
                    break
            else:
                model = platform.processor() or "unknown"

    return {
        "model":      model,
        "cores_phys": psutil.cpu_count(logical=False) if psutil else None,
        "cores_log":  psutil.cpu_count(logical=True)  if psutil else None,
        "max_mhz":    max_mhz,
    }

def _detect_ram() -> float | None:
    if psutil:
        return round(psutil.virtual_memory().total / 2**30, 1)
    return None

def _detect_disks() -> list[dict]:
    plat = platform.system()
    disks: list[dict] = []
    if plat == "Linux":
        lsblk = _run(["lsblk", "-dn", "-o", "NAME,SIZE,ROTA"])
        for line in lsblk.splitlines():
            name, size, rota = line.split()
            disks.append({
                "device": f"/dev/{name}",
                "size":   size,
                "type":   "HDD" if rota == "1" else "SSD/Flash"
            })
    elif plat == "Windows":
        output = _run(["wmic", "diskdrive", "get", "Model,Size,MediaType"])
        for line in output.splitlines()[1:]:
            if line.strip():
                *model, size = line.rsplit(None, 1)
                size_gb = round(int(size) / 2**30, 1)
                disks.append({"device": " ".join(model), "size": f"{size_gb}G", "type": "unknown"})
    elif plat == "Darwin":
        diskutil = _run(["diskutil", "info", "/"])
        size_match = re.search(r"Disk Size.*\((\d+\.\d+)\s*GB\)", diskutil)
        disks.append({
            "device": "/",
            "size":   f"{size_match.group(1)}G" if size_match else "unknown",
            "type":   "SSD/Flash"
        })
    return disks

def _docker_version() -> str | None:
    if shutil.which("docker"):
        return _run(["docker", "--version"])
    return None

def _compose_version() -> str | None:
    if shutil.which("docker"):
        out = _run(["docker", "compose", "version"])
        m = re.search(r"v?(\d+\.\d+\.\d+)", out)
        return m.group(1) if m else None
    return None

# ───────────────────────── Sammeln ──────────────────────────────
specs = {
    "cpu":     _detect_cpu(),
    "ram_gb":  _detect_ram(),
    "os":      f"{platform.system()} {platform.release()} ({platform.version()})",
    "kernel":  _run(["uname", "-srv"]) if shutil.which("uname") else None,
    "disks":   _detect_disks(),
    "docker":  _docker_version(),
    "compose": _compose_version(),
}

# ───────────────────────── Ausgabe ──────────────────────────────
if "--json" in sys.argv:
    print(json.dumps(specs, indent=2, ensure_ascii=False))
    sys.exit()

# Markdown-Tabelle
print("## Hardware- & Software-Umgebung\n")
print("| Komponente | Wert |")
print("|------------|------|")
cpu = specs["cpu"]
print(f"| CPU | {cpu['model']} ({cpu['cores_phys']} C / {cpu['cores_log']} T, "
      f"max {cpu['max_mhz']} MHz) |")
print(f"| RAM | {specs['ram_gb']} GB |")
for d in specs["disks"]:
    print(f"| Disk | {d['device']} – {d['size']} ({d['type']}) |")
print(f"| Betriebssystem | {specs['os']} |")
if specs["docker"]:
    print(f"| Docker Engine | {specs['docker']} |")
if specs["compose"]:
    print(f"| Docker Compose | {specs['compose']} |")
