"""Точка входа в программу."""
from common.logger import set_logger_config
from etl_process.main import ETLProcess


def main() -> None:
    set_logger_config()
    etl_process = ETLProcess()
    etl_process.start_process()


if __name__ == '__main__':
    main()
