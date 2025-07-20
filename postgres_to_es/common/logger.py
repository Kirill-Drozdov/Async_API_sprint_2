import logging
from pathlib import Path


def set_logger_config() -> None:
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)

    # Настройка логгера.
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Создаем форматтер с timestamp.
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    # Создаем файловый обработчик.
    file_handler = logging.FileHandler(
        filename=log_dir / 'etl_process.log',
        encoding='utf-8',
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    # Добавляем обработчик к логгеру.
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)
