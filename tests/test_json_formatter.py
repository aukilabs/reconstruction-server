import io
import json
import logging
import tempfile
import unittest
from pathlib import Path

from reconstruction.log import PACKAGE, JsonFormatter, configure, setup_logger


def _record(level=logging.WARNING, msg="hello %s", args=("world",)):
    return logging.LogRecord("some.logger", level, __file__, 1, msg, args, None)


class JsonFormatterTests(unittest.TestCase):
    """The Rust runners parse each stdout/stderr line as JSON and read "level" and "message"."""

    def test_line_shape(self):
        formatter = JsonFormatter(domain_id="d1", job_id="job_1", datefmt="%Y-%m-%dT%H:%M:%S")
        line = formatter.format(_record())
        self.assertNotIn("\n", line)
        record = json.loads(line)
        self.assertEqual(set(record), {"time", "level", "name", "tags", "message"})
        self.assertEqual(record["level"], "warning")
        self.assertEqual(record["name"], "some.logger")
        self.assertEqual(record["message"], "hello world")
        self.assertEqual(record["tags"], {"domain_id": "d1", "job_id": "job_1"})
        self.assertRegex(record["time"], r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{9}Z$")

    def test_dataset_id_tag(self):
        formatter = JsonFormatter(domain_id="d1", job_id="job_1", dataset_id="scan_a", datefmt="%Y-%m-%dT%H:%M:%S")
        record = json.loads(formatter.format(_record(level=logging.ERROR)))
        self.assertEqual(record["level"], "error")
        self.assertEqual(record["tags"], {"domain_id": "d1", "job_id": "job_1", "dataset_id": "scan_a"})

    def test_dataset_id_from_the_record(self):
        formatter = JsonFormatter(domain_id="d1", job_id="job_1", datefmt="%Y-%m-%dT%H:%M:%S")
        record = _record()
        record.dataset_id = "scan_b"
        self.assertEqual(json.loads(formatter.format(record))["tags"]["dataset_id"], "scan_b")

    def test_multiline_message_stays_one_line(self):
        formatter = JsonFormatter(domain_id="", job_id="", datefmt="%Y-%m-%dT%H:%M:%S")
        line = formatter.format(_record(msg="a\nb", args=()))
        self.assertNotIn("\n", line)
        self.assertEqual(json.loads(line)["message"], "a\nb")


class LoggerTreeTests(unittest.TestCase):
    """configure() sends every reconstruction.* record to stderr and log.txt, once each."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.log_txt = Path(self._tmp.name) / "log.txt"
        self.stream = io.StringIO()

    def tearDown(self):
        for name in (PACKAGE, "reconstruction.test_stage"):
            logger = logging.getLogger(name)
            for handler in list(logger.handlers):
                logger.removeHandler(handler)
                handler.close()
            for log_filter in list(logger.filters):
                logger.removeFilter(log_filter)
        self._tmp.cleanup()

    def _configure(self, **kwargs):
        logger = configure(log_file=self.log_txt, domain_id="d1", job_id="job_1", **kwargs)
        console = next(h for h in logger.handlers if not isinstance(h, logging.FileHandler))
        console.setStream(self.stream)
        return logger

    def _console_lines(self):
        return [line for line in self.stream.getvalue().splitlines() if line]

    def test_json_is_the_default_console_format(self):
        # The runners never pass --log_format, so production relies on this default.
        self._configure()
        logging.getLogger("reconstruction.some.module").info("hello")
        (line,) = self._console_lines()
        record = json.loads(line)
        self.assertEqual(record["name"], "reconstruction.some.module")
        self.assertEqual(record["message"], "hello")
        self.assertEqual(record["tags"], {"domain_id": "d1", "job_id": "job_1"})

    def test_text_format(self):
        self._configure(log_format="text")
        logging.getLogger("reconstruction.some.module").warning("hello")
        (line,) = self._console_lines()
        self.assertRegex(line, r" reconstruction\.some\.module WARNING hello$")

    def test_stage_logger_writes_once_with_dataset_tag_and_its_own_file(self):
        self._configure()
        stage_log = Path(self._tmp.name) / "local_logs"
        stage = setup_logger(name="reconstruction.test_stage", log_file=stage_log, dataset_id="scan_a")
        stage.info("from the stage")
        logging.getLogger("reconstruction.test_stage.child").info("from a module")

        first, second = (json.loads(line) for line in self._console_lines())
        self.assertEqual(first["tags"]["dataset_id"], "scan_a")
        self.assertNotIn("dataset_id", second["tags"])
        log_txt = self.log_txt.read_text()
        self.assertIn("from the stage", log_txt)
        self.assertIn("from a module", log_txt)
        self.assertIn("from the stage", stage_log.read_text())

    def test_reusing_a_stage_name_replaces_its_file_and_tag(self):
        self._configure()
        setup_logger(name="reconstruction.test_stage", log_file=Path(self._tmp.name) / "a", dataset_id="scan_a")
        stage = setup_logger(name="reconstruction.test_stage", log_file=Path(self._tmp.name) / "b", dataset_id="scan_b")
        self.assertEqual(len(stage.handlers), 1)
        stage.info("x")
        (line,) = self._console_lines()
        self.assertEqual(json.loads(line)["tags"]["dataset_id"], "scan_b")

    def test_records_do_not_reach_the_root_logger(self):
        # A library calling logging.basicConfig() used to make every local line print twice.
        root_stream = io.StringIO()
        root_handler = logging.StreamHandler(root_stream)
        logging.getLogger().addHandler(root_handler)
        try:
            self._configure()
            logging.getLogger("reconstruction.some.module").warning("once")
        finally:
            logging.getLogger().removeHandler(root_handler)
        self.assertEqual(root_stream.getvalue(), "")
        self.assertEqual(len(self._console_lines()), 1)


if __name__ == "__main__":
    unittest.main()
