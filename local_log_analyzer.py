import re
import os
import hashlib
import ollama

LOCAL_LOG_FILENAME = "AMQERR01.LOG"
MODEL_SLM = "qwen2.5-coder:0.5b"

def normalizza_log(line: str) -> str:
    """Removes timestamps, PIDs, and IP addresses to extract the structural error pattern."""
    pattern = re.sub(r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}', '', line)
    pattern = re.sub(r'Process\(\d+\.\d+\)', '', pattern)
    pattern = re.sub(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', '<IP>', pattern)
    return pattern.strip()

def calcola_hash(testo: str) -> str:
    """Generates a unique MD5 hash for deduplicating recurring anomalies."""
    return hashlib.md5(testo.encode('utf-8')).hexdigest()

def preleva_e_analizza_log_stream() -> list:
    """Reads the log file in streaming mode, filtering unique errors (E:) and warnings (W:)."""
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
    """Queries the local SLM to get a diagnosis, impact, and corrective MQSC commands."""
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
```[cite: 2]