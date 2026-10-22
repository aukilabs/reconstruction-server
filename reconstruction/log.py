"""Logging: JSON log lines for the runner, and log files.

Modules log through logging.getLogger(__name__), so every record lands under the "reconstruction"
logger. An entry point calls configure() once: from then on every record goes to stderr, one JSON
object per line, which the Rust runner parses for level and message, and to <job root>/log.txt,
which the runner uploads as the task log. Each stage also keeps its own log file through
setup_logger(): local_logs per scan, global_logs, update_logs.
"""
import json
import logging
import time

PACKAGE = "reconstruction"
# Stage loggers. refine.py and triangulation.py both log to LOCAL_REFINEMENT, so records of the
# scan being refined carry its dataset_id tag.
LOCAL_REFINEMENT = "reconstruction.pipeline.local"
GLOBAL_REFINEMENT = "reconstruction.pipeline.global_"
UPDATE_REFINEMENT = "reconstruction.pipeline.update"


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
        dataset_id = getattr(record, "dataset_id", None) or self.dataset_id
        if dataset_id:
            record_dict = {
                "time": s,
                "level": record.levelname.lower(),
                "name": record.name,
                "tags": {
                    "domain_id": self.domain_id,
                    "job_id": self.job_id,
                    "dataset_id": dataset_id},
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


def _console_formatter(log_format, domain_id, job_id):
    if log_format == "json":
        return JsonFormatter(
            datefmt='%Y-%m-%dT%H:%M:%S',
            domain_id=domain_id,
            job_id=job_id,
        )
    return logging.Formatter(fmt='%(asctime)s %(name)s %(levelname)s %(message)s')


def configure(log_file=None, domain_id="", job_id="", level="INFO", log_format="json"):
    """Send every reconstruction.* record to stderr and, if given, to log_file.

    Call once per run, from the entry point. Calling it again replaces the handlers.
    """
    logger = logging.getLogger(PACKAGE)
    logger.setLevel(getattr(logging, level.upper()))
    # Don't pass records on to the root logger, in case a library configured it.
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(_console_formatter(log_format, domain_id, job_id))
    logger.addHandler(console_handler)
    if log_file:
        add_file_handler(logger, log_file)
    return logger


class _DatasetTag(logging.Filter):
    def __init__(self, dataset_id):
        super().__init__()
        self.dataset_id = dataset_id

    def filter(self, record):
        record.dataset_id = self.dataset_id
        return True


def setup_logger(name=None, log_file=None, domain_id="", job_id="", dataset_id=None, level="INFO", log_format="json"):
    """A stage logger under "reconstruction", with its own log file.

    Its records also go to configure()'s stderr and log.txt handlers. Records logged through it
    carry dataset_id when given. If configure() has not run yet, this configures stderr only.
    """
    if not logging.getLogger(PACKAGE).handlers:
        configure(domain_id=domain_id, job_id=job_id, level=level, log_format=log_format)

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()),)

    # Clear existing handlers and tags (if reusing same name)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    for log_filter in list(logger.filters):
        logger.removeFilter(log_filter)

    if log_file:
        logger, _ = add_file_handler(logger, log_file)
    if dataset_id:
        logger.addFilter(_DatasetTag(dataset_id))

    return logger


def add_file_handler(logger, log_file):
    file_formatter = logging.Formatter(fmt='%(asctime)s %(name)s %(levelname)s %(message)s')
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    return logger, file_handler
