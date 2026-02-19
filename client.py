import time
from broker import RedisBroker
from models import Task, setup_logger

def generate_tasks(count: int = 5):
    broker = RedisBroker()
    logger = setup_logger("CLIENT")
    
    logger.info(f"Sisteme {count} adet yeni görev gönderiliyor...")
    
    for i in range(count):
        # Görev içeriği (payload) - Gerçek hayatta bu bir video işleme veya mail atma isteği olabilir
        payload = {"islem_tipi": "veri_analizi", "veri_id": 100 + i, "zorluk": "yuksek"}
        
        # Yeni task nesnesi oluştur ve kuyruğa at
        new_task = Task(payload=payload)
        broker.push_task(new_task)
        
        time.sleep(0.5) # Görevleri yarım saniye arayla gönder
        
    logger.info("Tüm görevler başarıyla kuyruğa iletildi!")

if __name__ == "__main__":
    generate_tasks(5)