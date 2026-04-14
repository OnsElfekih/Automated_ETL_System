import json
import datetime
import os

def log_event(event_type, details, filepath="logs/pipeline_logs.json"):
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event": event_type,
        "details": details
    }
    
    # Créer le répertoire s'il n'existe pas
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    # Ajouter le log en format JSON Lines (un objet JSON par ligne)
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(log_entry) + "\n")
