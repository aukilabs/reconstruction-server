"""Logging: JSON log lines for the runner, and log files."""
import json
import logging
import time


class JsonFormatter(logging.Formatter):
    """Formatter to dump error message into JSON"""

    def __init__(self, domain_id, job_id, dataset_id = None, fmt = None, datefmt = None, style = "%", validate = True):
        super().__init__(fmt, datefmt, style, validate)
        self.domain_id = domain_id
        self.job_id = job_id
        self.dataset_id = dataset_id

    def format(self, record: logging.LogRecord) -> str:
        t = time.strftime(self.datefmt, time.gmtime(record.created))
        s = '%s.%09dZ' % (t, record.msecs*1e6)
        if self.dataset_id:
            record_dict = {
                "time": s,
                "level": record.levelname.lower(),
                "name": record.name,
                "tags": {
                    "domain_id": self.domain_id,
                    "job_id": self.job_id,
                    "dataset_id": self.dataset_id},
                "message": record.getMessage()
            }
        else:
            record_dict = {
                "time": s,
                "level": record.levelname.lower(),
                "name": record.name,
                "tags": {
                    "domain_id": self.domain_id,
                    "job_id": self.job_id},
                "message": record.getMessage()
            }
        return json.dumps(record_dict)


def setup_logger(name=None, log_file=None, domain_id="", job_id="", dataset_id=None, level="INFO", log_format="json"):
    """To setup as many loggers as you want"""

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()),)

    # Clear existing handlers (if reusing same name)
    if logger.hasHandlers():
        logger.handlers.clear()

    if log_file:
        logger, _ = add_file_handler(logger, log_file)

    console_handler = logging.StreamHandler()
    if log_format == "json":
        console_handler.setFormatter(
            JsonFormatter(
                datefmt='%Y-%m-%dT%H:%M:%S',
                domain_id=domain_id,
                job_id=job_id,
                dataset_id=dataset_id
            )
        )
    else:
        console_handler.setFormatter(
            logging.Formatter(
                fmt='%(asctime)s %(name)s %(levelname)s %(message)s'
            )
        )


    logger.addHandler(console_handler)

    return logger


def add_file_handler(logger, log_file):
    file_formatter = logging.Formatter(fmt='%(asctime)s %(name)s %(levelname)s %(message)s')
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    return logger, file_handler
