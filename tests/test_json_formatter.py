import json
import logging
import unittest

from reconstruction.log import JsonFormatter, setup_logger


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

    def test_multiline_message_stays_one_line(self):
        formatter = JsonFormatter(domain_id="", job_id="", datefmt="%Y-%m-%dT%H:%M:%S")
        line = formatter.format(_record(msg="a\nb", args=()))
        self.assertNotIn("\n", line)
        self.assertEqual(json.loads(line)["message"], "a\nb")


class SetupLoggerTests(unittest.TestCase):
    def test_json_is_the_default_console_format(self):
        # The runners never pass --log_format, so production relies on this default.
        logger = setup_logger(name="test_json_formatter.default", domain_id="d1", job_id="job_1")
        (handler,) = logger.handlers
        self.assertIsInstance(handler.formatter, JsonFormatter)

    def test_text_format(self):
        logger = setup_logger(name="test_json_formatter.text", log_format="text")
        (handler,) = logger.handlers
        self.assertNotIsInstance(handler.formatter, JsonFormatter)

    def test_reusing_a_name_replaces_handlers(self):
        setup_logger(name="test_json_formatter.reuse")
        logger = setup_logger(name="test_json_formatter.reuse")
        self.assertEqual(len(logger.handlers), 1)


if __name__ == "__main__":
    unittest.main()
