"""Конфигурация Gunicorn для продакшена."""
import multiprocessing

from core.config import settings

# Базовые параметры.
bind = f'0.0.0.0:{settings.app_port}'
workers = (multiprocessing.cpu_count() * 2) + 1
worker_class = 'uvicorn.workers.UvicornWorker'
timeout = 120
keepalive = 5

# Безопасность:
# Ограничение количества заголовков.
limit_request_fields = 50
# Максимальный размер заголовка.
limit_request_field_size = 8190

# Логирование
# stdout
accesslog = '-'
# stdout
errorlog = '-'
