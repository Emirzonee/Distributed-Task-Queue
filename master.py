import time
from broker import RedisBroker
from models import setup_logger

class MasterOrchestrator:
    def __init__(self):
        self.broker = RedisBroker()
        self.logger = setup_logger("MASTER")

    def monitor_workers(self) -> None:
        self.logger.info("Orkestratör devrede. Sağlık kontrolü (Heartbeat) izleniyor...")
        
        while True:
            # O an üzerinde iş olan (processing) tüm Worker'ları bul
            active_workers = self.broker.get_all_processing_workers()
            
            for w_id in active_workers:
                # Eğer Worker'ın kalp atışı sönmüşse (TTL dolmuşsa)
                if not self.broker.is_worker_alive(w_id):
                    self.logger.warning(f"⚠️ [KAYIP] Worker-{w_id} yanıt vermiyor!")
                    self.logger.info(f"Worker-{w_id} için Redelivery (Geri Kazanım) başlatılıyor...")
                    
                    # Çöken işçinin elindeki görevleri ana kuyruğa geri taşı
                    recovered_count = self.broker.requeue_orphaned_tasks(w_id)
                    
                    self.logger.info(f"✅ Toplam {recovered_count} görev başarıyla kurtarıldı ve sıraya eklendi.")
            
            # Her 3 saniyede bir tarama yap
            time.sleep(3)

if __name__ == "__main__":
    master = MasterOrchestrator()
    try:
        master.monitor_workers()
    except KeyboardInterrupt:
        master.logger.info("Orkestratör durduruldu.")