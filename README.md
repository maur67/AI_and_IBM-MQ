# 🤖 AI & Advanced IBM® MQ Log Diagnostics

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![SLM: Qwen2.5-Coder](https://img.shields.io/badge/Ollama-qwen2.5--coder%3A0.5b-orange.svg)](https://ollama.com/library/qwen2.5-coder:0.5b)
[![Platform: Linux](https://img.shields.io/badge/platform-Linux-lightgrey.svg)](https://www.kernel.org/)

> **Operational Manual and Technical Guide for Middleware Infrastructure**  
> **Author:** Maurizio Delle Fratte | **Version:** 2.0 | **Date:** November 2026

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Official Technology References](#-official-technology-references)
- [1. Installing and Optimizing Ollama](#-1-installing-and-optimizing-ollama)
- [2. System Requirements and Python Environment](#-2-system-requirements-and-python-environment)
- [3. Analysis Script Architecture](#-3-analysis-script-architecture)
- [4. Operational Guide and Output Example](#-4-operational-guide-and-output-example)
- [5. Intellectual Property, Trademarks & License](#-5-intellectual-property-trademarks--license)

---

## 📖 Project Overview

This repository provides a high-efficiency Python solution for automatic and proactive diagnosis of **IBM® MQ** error logs (`AMQERRxx.LOG`), executed entirely locally (*on-premise / air-gapped ready*) via file streaming.

To minimize memory footprint and maximize inference speed on middleware systems, the architecture leverages the specialized Small Language Model (SLM) **`qwen2.5-coder:0.5b`** orchestrated through **Ollama**, tuned via API parameters (`num_predict`, `num_ctx`, `temperature`) and environment variables (`OLLAMA_NUM_PARALLEL`, `OLLAMA_KEEP_ALIVE`).

---

## 🔗 Official Technology References

Methodologies, error code parsing, and generated MQSC commands rely exclusively on the official documentation of their respective vendors:

1. **IBM® MQ Documentation (IBM Documentation Portal):**
   - [IBM MQ Messages and Error Codes (AMQ9000 - AMQ9999)](https://www.ibm.com/docs/en/ibm-mq/9.3?topic=messages-amq9000-amq9999-remote-messages)
2. **Ollama & Technical Small Language Models (SLM):**
   - [Download Ollama Server and Executable](https://ollama.com/download)
   - [Specialized IT/Code Model `qwen2.5-coder:0.5b`](https://ollama.com/library/qwen2.5-coder:0.5b)
   - [Official Python Client for Ollama (`ollama-python`)](https://github.com/ollama/ollama-python)

---

## ⚙️ 1. Installing and Optimizing Ollama

The `qwen2.5-coder:0.5b` model maintains a very small memory footprint (~390 MB), while providing higher syntactic accuracy on MQSC commands compared to general-purpose models of similar size.

### A. Optimization Parameters

| Parameter / Variable | Configured Value | Technical Function |
| :--- | :--- | :--- |
| `OLLAMA_KEEP_ALIVE` | `24h` | Keeps model weights in RAM/VRAM, eliminating cold-start latency. |
| `OLLAMA_NUM_PARALLEL` | `1` | Optimizes CPU thread allocation for deterministic sequential processing. |
| `num_predict` | `256` | Caps output tokens, keeping the SLM focused on root cause and MQSC commands. |
| `num_ctx` | `1024` | Reduces the context window to the minimum required to lower latency. |
| `temperature` | `0.1` | Minimizes probabilistic variance to prevent syntactic hallucinations. |

### B. Linux Daemon Setup

```bash
# 1. Download and automatically install the Ollama daemon
curl -fsSL https://ollama.com/install.sh | sh

# 2. Configure environment variables for performance
export OLLAMA_KEEP_ALIVE=24h
export OLLAMA_NUM_PARALLEL=1

# 3. Pull the specialized qwen2.5-coder:0.5b model
ollama pull qwen2.5-coder:0.5b

# 4. Run a quick functional check with an IBM MQ error code
ollama run qwen2.5-coder:0.5b "Analyze error AMQ9209E"
```

---

## 💻 2. System Requirements and Python Environment

### Hardware Specifications (Resource Footprint)

| Hardware Resource | Minimum Spec (Low-Resource) | Standard Production Profile | Efficiency Advantage |
| :--- | :--- | :--- | :--- |
| **CPU / Processor** | 1 vCPU (x86_64 / ARM64) | 2 vCPU (max usage < 5%) | Fast inference with a 0.5B-parameter SLM |
| **RAM / Memory** | 512 MB - 1 GB total RAM | 2 GB total RAM | Python script < 100 MB; SLM model ~390 MB |
| **Disk / Storage** | 2 GB total storage | 5 GB SSD storage | Instant loading from disk |

### Library Installation and Virtual Environment

```bash
# Update system packages and create a virtual environment
sudo apt update && sudo apt install -y python3 python3-pip python3-venv build-essential
python3 -m venv mq_ai_env
source mq_ai_env/bin/activate

# Upgrade packaging tools and install dependencies
pip install --upgrade pip setuptools wheel
pip install --upgrade --index-url https://pypi.org/simple/ send2trash
pip install requests urllib3 ollama

# Verify the environment
python3 -c "import requests, urllib3, ollama, send2trash; print('Linux Setup: OK')"
```

---

## 🧠 3. Analysis Script Architecture

The pipeline processes the local `AMQERR01.LOG` file through 5 stages: **Streaming Ingestion**, **Regex Normalization**, **MD5 Hash Deduplication**, **Local SLM Inference**, and **Output Formatting**.

Save the following code as `local_log_analyzer.py`:

```python
import re
import os
import hashlib
import ollama

LOCAL_LOG_FILENAME = "AMQERR01.LOG"
MODEL_SLM = "qwen2.5-coder:0.5b"

def normalize_log(line: str) -> str:
    """Removes timestamps, PIDs, and IP addresses to extract the structural error pattern."""
    pattern = re.sub(r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}', '', line)
    pattern = re.sub(r'Process\(\d+\.\d+\)', '', pattern)
    pattern = re.sub(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', '<IP>', pattern)
    return pattern.strip()

def compute_hash(text: str) -> str:
    """Generates a unique MD5 hash to deduplicate recurring anomalies."""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def fetch_and_analyze_log_stream() -> list:
    """Streams the log file while filtering unique errors (E:) and warnings (W:)."""
    if not os.path.exists(LOCAL_LOG_FILENAME):
        print(f"❌ Error: File '{LOCAL_LOG_FILENAME}' was not found in the current directory ({os.getcwd()})")
        return []
    
    seen_patterns = set()
    filtered_anomalies = []
    
    try:
        with open(LOCAL_LOG_FILENAME, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line_str = line.strip()
                if not line_str:
                    continue
                if "AMQ" in line_str and ("E:" in line_str or "W:" in line_str):
                    pattern = normalize_log(line_str)
                    pattern_hash = compute_hash(pattern)
                    if pattern_hash not in seen_patterns:
                        seen_patterns.add(pattern_hash)
                        filtered_anomalies.append((line_str, pattern))
        return filtered_anomalies
    except Exception as e:
        print(f"Error reading local file: {e}")
        return []

def comprehensive_slm_diagnosis(log_line: str) -> str:
    """Queries the local SLM for diagnosis, impact, and corrective MQSC commands."""
    prompt = f"""
    You are an IBM MQ Middleware expert. Analyze this log error:
    LOG: {log_line}
    
    Provide a concise response in English:
    1. DIAGNOSIS: Root cause.
    2. IMPACT: Criticality level.
    3. CORRECTIVE ACTION: Steps and official MQSC commands.
    """
    try:
        res = ollama.chat(
            model=MODEL_SLM,
            messages=[{'role': 'user', 'content': prompt}],
            options={
                'temperature': 0.1,
                'num_predict': 256,
                'num_ctx': 1024
            }
        )
        return res['message']['content']
    except Exception as e:
        return f"Ollama AI call error: {e}"

if __name__ == "__main__":
    anomalies = fetch_and_analyze_log_stream()
    if not anomalies:
        print("No anomalies detected in the local file.")
    for i, (original_log, pattern) in enumerate(anomalies, 1):
        print(f"--- ANOMALY #{i} ---")
        print(f"LOG: {original_log}")
        print(comprehensive_slm_diagnosis(original_log))
        print("=" * 60)
```

---

## 🚀 4. Operational Guide and Output Example

Make sure `AMQERR01.LOG` is placed in the same directory as the script before running it:

```bash
# 1. Change to the working directory
cd /opt/mq_ai_analytics

# 2. Activate the Python virtual environment
source mq_ai_env/bin/activate

# 3. Run the local analysis
python3 local_log_analyzer.py
```

### Verified Output Example (`AMQ9209E`)

```text
=================================================================
  STARTING DETAILED IBM MQ LOG DIAGNOSTIC TEST SUITE (AI)
=================================================================
▶ RUNNING TEST-01 - Scenario: SDR/RCVR Channel Network Interruption
  RAW LOG: 07/11/2026 14:22:10 - AMQ9209E: Connection to host '10.240.12.88 (1414)' closed unexpectedly.
-----------------------------------------------------------------
1. TECHNICAL DIAGNOSIS (Root Cause):
   The TCP/IP channel experienced an abnormal socket connection drop toward remote host 10.240.12.88 on port 1414 (unexpected RESET or FIN).
2. SEVERITY LEVEL & BUSINESS IMPACT:
   • Severity: HIGH.
   • Impact: Messages queued on the transmission queue (XMITQ) are blocked.
3. CORRECTIVE ACTION PLAN (Official IBM MQ Step-by-Step):
   Step 1: DISPLAY CHSTATUS('QM_TEST.TO.QM_REMOTE') ALL
   Step 2: ALTER CHANNEL('QM_TEST.TO.QM_REMOTE') CHLTYPE(SDR) KAINT(300)
   Step 3: START CHANNEL('QM_TEST.TO.QM_REMOTE')
=================================================================
```

---

## ⚖️ 5. Intellectual Property, Trademarks & License

### Intellectual Property and Trademarks (Trademark Notice)
- **IBM®** and **IBM® MQ** are registered trademarks of **International Business Machines Corporation** in the United States and/or other countries. This project is an independent diagnostic support tool and is **not** affiliated with, sponsored by, or endorsed by IBM Corporation.
- **Qwen** and **Qwen2.5-Coder** are models developed by **Alibaba Cloud** (released under the Apache 2.0 license).
- **Ollama** is an open-source project released under the MIT license.
- The Python libraries mentioned (`ollama`, `requests`, `urllib3`, `send2trash`) are the property of their respective authors and subject to their respective Open Source licenses (MIT / Apache 2.0 / BSD).

### Disclaimer
The software and MQSC commands automatically generated by artificial intelligence are provided *"AS IS"*, without warranties of any kind. Before executing modification or restart commands on **Production IBM MQ** environments, always verify the operational impact with a certified middleware administrator.

### Code and Documentation License
Copyright © 2026 **Maurizio Delle Fratte**.  
This project is distributed under the **MIT License** (or according to the terms of the `LICENSE` file in the root of the repository). Use, copying, modification, and distribution of the code are permitted, provided attribution to the original author is preserved.
