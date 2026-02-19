import os
import redis
from typing import Optional, List
from dotenv import load_dotenv
from models import Task, setup_logger

# Şifre kasasını (.env) sisteme yükle
load_dotenv()

class RedisBroker:
    def __init__(self):
        # Şifreyi güvenlik kasasından çekiyoruz
        redis_url = os.getenv("REDIS_URL")
        
        if not redis_url:
            raise ValueError("Kritik Hata: REDIS_URL bulunamadı! Lütfen .env dosyasını kontrol edin.")
            
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self.logger = setup_logger("BROKER")
        self.main_queue = "queue:pending"

# ... (Aşağıdaki kısımlar tamamen aynı kalacak, sadece üst tarafı değiştirdik)

    def push_task(self, task: Task) -> None:
        """Yeni görevi ana kuyruğa ekler."""
        self.redis.lpush(self.main_queue, task.to_json())
        self.logger.info(f"Task kuyruğa eklendi. ID: {task.task_id}")

    def fetch_task(self, worker_id: str, timeout: int = 0) -> Optional[Task]:
        """
        RELIABLE QUEUE DESENİ: Görevi ana kuyruktan alır ve güvenli bir şekilde 
        Worker'ın processing kuyruğuna taşır (Atomik işlem).
        """
        processing_queue = f"queue:processing:{worker_id}"
        
        task_data = self.redis.blmove(
            self.main_queue,       # 1. Parametre: Kaynak kuyruk
            processing_queue,      # 2. Parametre: Hedef kuyruk
            timeout=timeout,
            src="RIGHT",           # Kaynağın sağından al
            dest="LEFT"            # Hedefin soluna koy
        )
        
        if task_data:
            return Task.from_json(task_data)
        return None

    def complete_task(self, worker_id: str, task: Task) -> None:
        """Görev başarıyla bittiğinde processing kuyruğundan tamamen siler."""
        processing_queue = f"queue:processing:{worker_id}"
        self.redis.lrem(processing_queue, 1, task.to_json())
        self.logger.info(f"Task başarıyla tamamlandı ve silindi. ID: {task.task_id}")

    # --- HEARTBEAT VE MASTER İÇİN YARDIMCI METOTLAR ---

    def set_heartbeat(self, worker_id: str, ttl: int = 5) -> None:
        """Worker'ın yaşadığını Redis'e bildirir."""
        self.redis.set(f"worker:heartbeat:{worker_id}", "ALIVE", ex=ttl)

    def is_worker_alive(self, worker_id: str) -> bool:
        """Master için Worker'ın yaşayıp yaşamadığını kontrol eder."""
        return bool(self.redis.exists(f"worker:heartbeat:{worker_id}"))

    def get_all_processing_workers(self) -> List[str]:
        """Şu an elinde iş olan tüm Worker ID'lerini bulur."""
        keys = self.redis.keys("queue:processing:*")
        return [k.split(":")[-1] for k in keys]

    def requeue_orphaned_tasks(self, worker_id: str) -> int:
        """Çöken Worker'ın elinde kalan işleri ana kuyruğa geri atar (Redelivery)."""
        processing_queue = f"queue:processing:{worker_id}"
        requeued_count = 0
        
        while True:
            # İşleniyor kuyruğundan ana kuyruğa geri taşı
            task_data = self.redis.rpoplpush(processing_queue, self.main_queue)
            if not task_data:
                break
            requeued_count += 1
            
        return requeued_count