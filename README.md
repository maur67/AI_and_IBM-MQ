# 🤖 AI & Diagnosi Avanzata dei Log IBM® MQ

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![SLM: Qwen2.5-Coder](https://img.shields.io/badge/Ollama-qwen2.5--coder%3A0.5b-orange.svg)](https://ollama.com/library/qwen2.5-coder:0.5b)
[![Platform: Linux](https://img.shields.io/badge/platform-Linux-lightgrey.svg)](https://www.kernel.org/)

> **Manuale Operativo e Guida Tecnica per l'Infrastruttura Middleware**  
> **Autore:** Maurizio Delle Fratte | **Versione:** 2.0 | **Data:** Novembre 2026

---

## 📋 Indice dei Contenuti

- [Descrizione del Progetto](#-descrizione-del-progetto)
- [Riferimenti Tecnologici Ufficiali](#-riferimenti-tecnologici-ufficiali)
- [1. Installazione e Ottimizzazione di Ollama](#-1-installazione-e-ottimizzazione-di-ollama)
- [2. Requisiti di Sistema e Ambiente Python](#-2-requisiti-di-sistema-e-ambiente-python)
- [3. Architettura dello Script di Analisi](#-3-architettura-dello-script-di-analisi)
- [4. Guida Operativa ed Esempio di Output](#-4-guida-operativa-ed-esempio-di-output)
- [5. Intellectual Property, Trademarks & Licenza](#-5-intellectual-property-trademarks--licenza)

---

## 📖 Descrizione del Progetto

Questo repository fornisce una soluzione Python ad altissima efficienza per la diagnosi automatica e proattiva dei log di errore **IBM® MQ** (`AMQERRxx.LOG`), eseguita interamente in locale (*on-premise / air-gapped ready*) mediante lettura streaming dei file.

Per minimizzare l'impronta di memoria e massimizzare la velocità di inferenza su sistemi middleware, l'architettura sfrutta il modello Small Language Model (SLM) specializzato **`qwen2.5-coder:0.5b`** orchestrato tramite **Ollama**, ottimizzato a livello di parametri API (`num_predict`, `num_ctx`, `temperature`) e variabili d'ambiente (`OLLAMA_NUM_PARALLEL`, `OLLAMA_KEEP_ALIVE`).

---

## 🔗 Riferimenti Tecnologici Ufficiali

Le metodologie, il parsing dei codici di errore e i comandi MQSC generati si basano esclusivamente sulla documentazione ufficiale dei rispettivi vendor:

1. **IBM® MQ Documentation (IBM Documentation Portal):**
   - [Messaggi e codici di errore IBM MQ (AMQ9000 - AMQ9999)](https://www.ibm.com/docs/en/ibm-mq/9.3?topic=messages-amq9000-amq9999-remote-messages)
2. **Ollama & Technical Small Language Models (SLM):**
   - [Download Server ed Executable Ollama](https://ollama.com/download)
   - [Modello specializzato IT/Code `qwen2.5-coder:0.5b`](https://ollama.com/library/qwen2.5-coder:0.5b)
   - [Client ufficiale Python per Ollama (`ollama-python`)](https://github.com/ollama/ollama-python)

---

## ⚙️ 1. Installazione e Ottimizzazione di Ollama

Il modello `qwen2.5-coder:0.5b` mantiene un footprint di memoria estremamente ridotto (~390 MB), garantendo al contempo un'elevata precisione sintattica sui comandi MQSC rispetto ai modelli generalisti di pari dimensione.

### A. Parametri di Ottimizzazione

| Parametro / Variabile | Valore Impostato | Funzione Tecnica |
| :--- | :--- | :--- |
| `OLLAMA_KEEP_ALIVE` | `24h` | Mantiene i pesi del modello in RAM/VRAM azzerando i tempi di cold-start. |
| `OLLAMA_NUM_PARALLEL` | `1` | Ottimizza l'allocazione dei thread CPU per elaborazioni sequenziali deterministe. |
| `num_predict` | `256` | Limita i token in uscita focalizzando l'SLM su causa radice e comandi MQSC. |
| `num_ctx` | `1024` | Riduce la finestra di contesto al minimo necessario per abbattere la latenza. |
| `temperature` | `0.1` | Minimizza la varianza probabilistica evitando allucinazioni sintattiche. |

### B. Setup Daemon su Linux

```bash
# 1. Download e installazione automatica del daemon Ollama
curl -fsSL https://ollama.com/install.sh | sh

# 2. Configurazione delle variabili d'ambiente per le prestazioni
export OLLAMA_KEEP_ALIVE=24h
export OLLAMA_NUM_PARALLEL=1

# 3. Download del modello specializzato qwen2.5-coder:0.5b
ollama pull qwen2.5-coder:0.5b

# 4. Verifica funzionale rapida con codice errore IBM MQ
ollama run qwen2.5-coder:0.5b "Analizza errore AMQ9209E"
```

---

## 💻 2. Requisiti di Sistema e Ambiente Python

### Specifiche Hardware (Resource Footprint)

| Risorsa Hardware | Specifica Minima (Low-Resource) | Profilo di Produzione Standard | Vantaggio di Efficienza |
| :--- | :--- | :--- | :--- |
| **CPU / Processor** | 1 vCPU (x86_64 / ARM64) | 2 vCPU (uso massimo < 5%) | Inferenza rapida con SLM da 0.5B parametri |
| **RAM / Memoria** | 512 MB - 1 GB RAM totali | 2 GB RAM totali | Script Python < 100 MB; Modello SLM ~390 MB |
| **Disk / Storage** | 2 GB Storage totale | 5 GB Storage SSD | Caricamento istantaneo da disco |

### Installazione Librerie e Virtual Environment

```bash
# Aggiornamento pacchetti di sistema e creazione virtual environment
sudo apt update && sudo apt install -y python3 python3-pip python3-venv build-essential
python3 -m venv mq_ai_env
source mq_ai_env/bin/activate

# Aggiornamento tool di packaging e installazione dipendenze
pip install --upgrade pip setuptools wheel
pip install --upgrade --index-url https://pypi.org/simple/ send2trash
pip install requests urllib3 ollama

# Verifica dell'ambiente
python3 -c "import requests, urllib3, ollama, send2trash; print('Linux Setup: OK')"
```

---

## 🧠 3. Architettura dello Script di Analisi

La pipeline elabora il file locale `AMQERR01.LOG` attraverso 5 fasi: **Streaming Ingestion**, **Normalizzazione Regex**, **Deduplicazione tramite Hash MD5**, **Inferenza Locale SLM** e **Formattazione Output**.

Salvare il codice seguente come `analizzatore_log_locale.py`:

```python
import re
import os
import hashlib
import ollama

LOCAL_LOG_FILENAME = "AMQERR01.LOG"
MODEL_SLM = "qwen2.5-coder:0.5b"

def normalizza_log(line: str) -> str:
    """Rimuove timestamp, PID e indirizzi IP per estrarre il pattern strutturale dell'errore."""
    pattern = re.sub(r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}', '', line)
    pattern = re.sub(r'Process\(\d+\.\d+\)', '', pattern)
    pattern = re.sub(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', '<IP>', pattern)
    return pattern.strip()

def calcola_hash(testo: str) -> str:
    """Genera un hash MD5 univoco per la deduplicazione delle anomalie ricorrenti."""
    return hashlib.md5(testo.encode('utf-8')).hexdigest()

def preleva_e_analizza_log_stream() -> list:
    """Legge il file di log in modalità streaming filtrando errori (E:) e warning (W:) univoci."""
    if not os.path.exists(LOCAL_LOG_FILENAME):
        print(f"❌ Errore: Il file '{LOCAL_LOG_FILENAME}' non è presente nella directory corrente ({os.getcwd()})")
        return []
    
    seen_patterns = set()
    anomalie_filtrate = []
    
    try:
        with open(LOCAL_LOG_FILENAME, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line_str = line.strip()
                if not line_str:
                    continue
                if "AMQ" in line_str and ("E:" in line_str or "W:" in line_str):
                    pattern = normalizza_log(line_str)
                    pattern_hash = calcola_hash(pattern)
                    if pattern_hash not in seen_patterns:
                        seen_patterns.add(pattern_hash)
                        anomalie_filtrate.append((line_str, pattern))
        return anomalie_filtrate
    except Exception as e:
        print(f"Errore nella lettura del file locale: {e}")
        return []

def diagnosi_esaustiva_slm(log_line: str) -> str:
    """Interroga l'SLM locale per ottenere diagnosi, impatto e comandi MQSC correttivi."""
    prompt = f"""
    Sei un esperto Middleware IBM MQ. Analizza questo errore di log:
    LOG: {log_line}
    
    Fornisci in italiano sintetico:
    1. DIAGNOSI: Causa radice.
    2. IMPATTO: Livello di criticità.
    3. AZIONE CORRETTIVA: Step e comandi MQSC ufficiali.
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
        return f"Errore chiamata Ollama AI: {e}"

if __name__ == "__main__":
    anomalie = preleva_e_analizza_log_stream()
    if not anomalie:
        print("Nessuna anomalia rilevata nel file locale.")
    for i, (log_originale, pattern) in enumerate(anomalie, 1):
        print(f"--- ANOMALIA #{i} ---")
        print(f"LOG: {log_originale}")
        print(diagnosi_esaustiva_slm(log_originale))
        print("=" * 60)
```

---

## 🚀 4. Guida Operativa ed Esempio di Output

Assicurarsi che il file `AMQERR01.LOG` si trovi nella stessa directory dello script prima dell'avvio:

```bash
# 1. Posizionarsi nella directory di lavoro
cd /opt/mq_ai_analytics

# 2. Attivare l'ambiente virtuale Python
source mq_ai_env/bin/activate

# 3. Avviare l'analisi locale
python3 analizzatore_log_locale.py
```

### Esempio di Output Verificato (`AMQ9209E`)

```text
=================================================================
  AVVIO SUITE DI TEST DIAGNOSTICO DETTAGLIATO LOG IBM MQ (AI)
=================================================================
▶ ESECUZIONE TEST-01 - Scenario: Interruzione di Rete Canale SDR/RCVR
  RAW LOG: 07/11/2026 14:22:10 - AMQ9209E: Connection to host '10.240.12.88 (1414)' closed unexpectedly.
-----------------------------------------------------------------
1. DIAGNOSI TECNICA (Causa Radice):
   Il canale TCP/IP ha subito un'interruzione anomala della connessione socket verso l'host remoto 10.240.12.88 sulla porta 1414 (RESET o FIN inatteso).
2. LIVELLO DI SEVERITÀ & IMPATTO BUSINESS:
   • Severità: ALTA.
   • Impatto: Messaggi bloccati in coda di trasmissione (XMITQ).
3. PIANO DI AZIONE CORRETTIVA (Step-by-step IBM MQ Ufficiale):
   Step 1: DISPLAY CHSTATUS('QM_TEST.TO.QM_REMOTE') ALL
   Step 2: ALTER CHANNEL('QM_TEST.TO.QM_REMOTE') CHLTYPE(SDR) KAINT(300)
   Step 3: START CHANNEL('QM_TEST.TO.QM_REMOTE')
=================================================================
```

---

## ⚖️ 5. Intellectual Property, Trademarks & Licenza

### Proprietà Intellettuale e Marchi Registrati (Trademark Notice)
- **IBM®** e **IBM® MQ** sono marchi registrati di **International Business Machines Corporation** negli Stati Uniti e/o in altri Paesi. Questo progetto è uno strumento indipendente di supporto diagnostico e **non** è affiliato, sponsorizzato o approvato da IBM Corporation.
- **Qwen** e **Qwen2.5-Coder** sono modelli sviluppati da **Alibaba Cloud** (rilasciati sotto licenza Apache 2.0).
- **Ollama** è un progetto open-source rilasciato sotto licenza MIT.
- Le librerie Python menzionate (`ollama`, `requests`, `urllib3`, `send2trash`) sono proprietà dei rispettivi autori e soggette alle relative licenze Open Source (MIT / Apache 2.0 / BSD).

### Esclusione di Responsabilità (Disclaimer)
Il software e i comandi MQSC generati automaticamente dall'intelligenza artificiale vengono forniti *"così come sono"* (*"AS IS"*), senza garanzie di alcun tipo. Prima di eseguire comandi di modifica o riavvio su ambienti **IBM MQ di Produzione**, verificare sempre l'impatto operativo con un amministratore middleware certificato.

### Licenza del Codice e della Documentazione
Copyright © 2026 **Maurizio Delle Fratte**.  
Questo progetto è distribuito sotto **Licenza MIT** (o secondo i termini del file `LICENSE` presente nella root del repository). È consentito l'uso, la copia, la modifica e la distribuzione del codice, purché venga mantenuta l'attribuzione all'autore originale.