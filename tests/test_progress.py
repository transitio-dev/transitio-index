"""Unit tests for the build's logging setup and progress helper."""

import io
import logging

import pytest

from transitio_index import progress


@pytest.fixture
def reset_package_logger():
    """Undo whatever ``configure`` did to the package logger after a test."""
    yield
    logger = logging.getLogger(progress.LOGGER_NAME)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    logger.setLevel(logging.NOTSET)
    logger.propagate = True


@pytest.fixture
def capturing_logger():
    """A private logger whose rendered messages are captured into a list."""
    logger = logging.getLogger("test.progress")
    logger.propagate = False
    logger.setLevel(logging.INFO)
    messages = []
    handler = logging.Handler()
    handler.emit = lambda record: messages.append(record.getMessage())
    logger.addHandler(handler)
    try:
        yield logger, messages
    finally:
        logger.removeHandler(handler)


def test_resolve_level():
    assert progress.resolve_level() == logging.INFO
    assert progress.resolve_level(verbose=True) == logging.DEBUG
    assert progress.resolve_level(quiet=True) == logging.WARNING


def test_progress_yields_all_items_and_logs_a_final_count(capturing_logger):
    logger, messages = capturing_logger
    # A huge interval means only the final "done" line is logged.
    out = list(
        progress.progress([10, 20, 30], "cut", total=3, interval=1e9, logger=logger)
    )
    assert out == [10, 20, 30]
    assert messages[-1] == "cut: 3/3 done"


def test_progress_without_a_total_reports_the_count(capturing_logger):
    logger, messages = capturing_logger
    list(progress.progress(iter("ab"), "scan", interval=1e9, logger=logger))
    assert messages[-1] == "scan: 2 done"


def test_progress_with_a_zero_total_reports_zero_over_zero(capturing_logger):
    logger, messages = capturing_logger
    list(progress.progress([], "empty", total=0, interval=1e9, logger=logger))
    assert messages[-1] == "empty: 0/0 done"


def test_ascii_safe_escapes_non_ascii():
    assert progress.ascii_safe("café 中") == "caf\\xe9 \\u4e2d"


def test_ascii_safe_escapes_control_characters():
    # An ANSI escape or tab in untrusted catalogue text must not reach the
    # terminal raw; printable ASCII (including spaces) is kept.
    assert progress.ascii_safe("a\x1b[31m X\tY") == "a\\x1b[31m X\\x09Y"


def test_control_safe_keeps_unicode_but_escapes_controls():
    # For the file: real characters are kept, control characters escaped.
    assert progress.control_safe("café\n中\tX\x1b") == "café\\x0a中\\x09X\\x1b"


def test_console_formatter_is_ascii_safe():
    formatter = progress._AsciiFormatter("%(message)s")
    record = logging.LogRecord("t", logging.INFO, __file__, 0, "café 中", None, None)
    out = formatter.format(record)
    out.encode("ascii")  # a strict ASCII console must not fail on it
    assert out == progress.ascii_safe("café 中")


def test_fatal_writes_one_ascii_safe_line():
    buffer = io.StringIO()
    progress.fatal("boom\nsecond\tline café", stream=buffer)
    assert buffer.getvalue() == "boom second\\x09line caf\\xe9\n"


def test_configure_writes_to_a_file_and_is_idempotent(tmp_path, reset_package_logger):
    log_file = tmp_path / "logs" / "build.log"  # the parent is created by configure
    logger = progress.configure(log_file=log_file, console=False)
    logger.info("hello")
    progress.configure(log_file=log_file, console=False)  # a second call must not dup
    logger.info("world")
    for handler in logger.handlers:
        handler.flush()
    text = log_file.read_text(encoding="utf-8")
    assert "hello" in text
    assert text.count("world") == 1


def test_configure_keeps_unicode_in_the_file(tmp_path, reset_package_logger):
    log_file = tmp_path / "unicode.log"
    logger = progress.configure(log_file=log_file, console=False)
    logger.info("café 中")  # the file is UTF-8 and keeps the real characters
    for handler in logger.handlers:
        handler.flush()
    assert "café 中" in log_file.read_text(encoding="utf-8")


def test_configure_file_tolerates_unencodable_text(tmp_path, reset_package_logger):
    # A Unix filename with non-UTF-8 bytes reaches logging as a surrogate; the
    # file handler must escape it rather than raise and drop the record.
    log_file = tmp_path / "surrogate.log"
    logger = progress.configure(log_file=log_file, console=False)
    logger.info("bad\udcffname")
    for handler in logger.handlers:
        handler.flush()
    text = log_file.read_text(encoding="utf-8")
    assert "bad" in text and "name" in text


def test_file_log_escapes_control_characters(tmp_path, reset_package_logger):
    log_file = tmp_path / "control.log"
    logger = progress.configure(log_file=log_file, console=False)
    logger.info("x\ny\x1bz")  # a newline or ESC in untrusted text is escaped
    for handler in logger.handlers:
        handler.flush()
    text = log_file.read_text(encoding="utf-8")
    assert "x\\x0ay\\x1bz" in text
    assert "\x1b" not in text
