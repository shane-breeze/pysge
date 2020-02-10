from .interface import (
    local_submit, mp_submit,
    batch_submit, batch_submit_yield, batch_resume,
)

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    del handler
    del formatter
del logger
