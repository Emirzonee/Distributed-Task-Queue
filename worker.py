import asyncio
import sys
from broker import RedisBroker
from models import setup_logger

class Worker:
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.broker = RedisBroker()
        self.logger = setup_logger(f"WORKER-{worker_id}")
        self.is_running = False

    async def heartbeat_loop(self) -> None:
        """Arka planda her 2 saniyede bir kalp atışını yeniler (TTL 5 sn)."""
        while self.is_running:
            self.broker.set_heartbeat(self.worker_id, ttl=5)
            await asyncio.sleep(2) 

    async def process_tasks(self) -> None:
        """Kuyruktan görev çeker ve işler."""
        while self.is_running:
            # 1 saniye timeout ile görev bekle (Reliable Queue)
            task = self.broker.fetch_task(self.worker_id, timeout=1)
            
            if task:
                self.logger.info(f"Görev Alındı: {task.payload} | TaskID: {task.task_id}")
                
                # Gerçek hayattaki ağır bir işlemi simüle ediyoruz
                await asyncio.sleep(3) 
                
                self.broker.complete_task(self.worker_id, task)
            else:
                # Kuyruk boşsa biraz dinlen
                await asyncio.sleep(0.5)

    async def start(self) -> None:
        self.logger.info(f"Worker [{self.worker_id}] göreve hazır!")
        self.is_running = True
        
        # Kalp atışını ve görev işleyiciyi paralel (asenkron) olarak çalıştır
        await asyncio.gather(
            self.heartbeat_loop(),
            self.process_tasks()
        )

if __name__ == "__main__":
    # Terminalden isim al, yoksa "W1" yap
    w_id = sys.argv[1] if len(sys.argv) > 1 else "W1"
    worker = Worker(w_id)
    
    try:
        asyncio.run(worker.start())
    except KeyboardInterrupt:
        worker.logger.info("Worker manuel olarak durduruldu.")