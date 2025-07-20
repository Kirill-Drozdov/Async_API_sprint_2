"""Точка входа для отладки приложения."""
import logging

import uvicorn

from core.config import settings
from core.logger import LOGGING

if __name__ == '__main__':
    uvicorn.run(
        'core.app:app',
        host='0.0.0.0',
        port=settings.app_port,
        log_config=LOGGING,
        log_level=logging.DEBUG,
        reload=True,
    )
