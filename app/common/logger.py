import logging

def get_logger(name: str = "app", level: int = logging.INFO) -> logging.Logger:
    """
    공통 로거 생성 함수. 이미 핸들러가 있으면 중복 추가하지 않음.
    """
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(name)s] %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger
