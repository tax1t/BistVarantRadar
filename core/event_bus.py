from datetime import datetime

class EventBus:
    events = []

    def __init__(self):
        pass

    def emit(self, event_type, data=None):
        timestamp = datetime.now().strftime("%H:%M:%S")
        EventBus.events.append({
            "timestamp": timestamp,
            "type": event_type,
            "payload": data
        })

    @classmethod
    def publish(cls, event_type, data=None):
        timestamp = datetime.now().strftime("%H:%M:%S")
        cls.events.append({
            "timestamp": timestamp,
            "type": event_type,
            "payload": data
        })

    @classmethod
    def get_recent_events(cls, limit=10):
        if not isinstance(cls.events, list):
            return []
        
        safe_events = []
        for e in cls.events[-limit:]:
            # Eğer gelen öğe bir sözlük değilse bile string'e çevirip patlamasını önleyelim
            if not isinstance(e, dict):
                e = {"type": "GENERIC_EVENT", "payload": str(e)}
            
            # Timestamp kesinlikle var olacak, yoksa varsayılan atanacak
            safe_events.append({
                "timestamp": e.get("timestamp", datetime.now().strftime("%H:%M:%S")),
                "type": e.get("type", "BİLGİ"),
                "payload": e.get("payload", e.get("data", None))
            })
        return safe_events