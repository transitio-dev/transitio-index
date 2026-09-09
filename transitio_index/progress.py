"""Screen and/or file logging for the build, with a progress helper.

``configure`` installs handlers on the ``transitio_index`` logger — the screen
(stderr), a file, or both at once — and the rest of the build logs through
``logging.getLogger(__name__)`` so every message flows to whatever was set up.
``build`` logs a start/finish line per stage; a long stage can wrap its loop
with ``progress`` to report how far it has got.

Logs go to stderr so the machine-readable JSON stage summaries on stdout stay
clean.
"""

import logging
import os
import sys
import time
from collections.abc import Sized

LOGGER_NAME = "transitio_index"


def resolve_level(verbose=False, quiet=False):
    """The log level for the ``--verbose`` / ``--quiet`` flags (default INFO)."""
    if verbose:
        return logging.DEBUG
    if quiet:
        return logging.WARNING
    return logging.INFO


def _escape(char):
    code = ord(char)
    if code <= 0xFF:
        return f"\\x{code:02x}"
    if code <= 0xFFFF:
        return f"\\u{code:04x}"
    return f"\\U{code:08x}"


def ascii_safe(text):
    """Escape non-ASCII and control characters to printable ASCII.

    Keeps a message on one printable-ASCII line, so untrusted catalogue text (a
    feed name or URL) in a console log line or fatal error cannot break or
    forge terminal output, nor fail to encode on a non-UTF-8 console.
    """
    return "".join(
        char if char.isascii() and char.isprintable() else _escape(char)
        for char in text
    )


def control_safe(text):
    """Escape control characters, keeping printable characters (ASCII or not).

    For the file log: preserves real Unicode (a feed name in its own script)
    while escaping newlines, ANSI escapes and other controls, so untrusted
    catalogue text cannot forge a record or inject terminal sequences when the
    file is later viewed.
    """
    return "".join(char if char.isprintable() else _escape(char) for char in text)


def fatal(message, *, stream=None):
    """Print a fatal error as one printable-ASCII line (defaults to stderr)."""
    print(ascii_safe(" ".join(message.splitlines())), file=stream or sys.stderr)


class _AsciiFormatter(logging.Formatter):
    """Escape non-ASCII and control characters, for the console handler."""

    def format(self, record):
        return ascii_safe(super().format(record))


class _ControlFormatter(logging.Formatter):
    """Escape control characters, keeping printable Unicode, for the file."""

    def format(self, record):
        return control_safe(super().format(record))


def configure(level=logging.INFO, *, log_file=None, console=True):
    """Route the build's logs to the screen, a file, or both.

    Idempotent: the package logger's handlers are replaced, so a second call —
    or a test — does not double every line. ``log_file``'s parent directory is
    created if needed and the file is appended to. Returns the package logger.
    """
    logger = logging.getLogger(LOGGER_NAME)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    logger.setLevel(level)
    logger.propagate = False
    fmt, datefmt = "%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"
    if console:
        # Escape non-ASCII on the console so a redirected, non-UTF-8 stderr (as
        # on Windows) cannot raise UnicodeEncodeError on a stray unicode value;
        # the file keeps the real characters (UTF-8).
        stream = logging.StreamHandler()
        stream.setFormatter(_AsciiFormatter(fmt, datefmt))
        logger.addHandler(stream)
    if log_file is not None:
        parent = os.path.dirname(os.fspath(log_file))
        if parent:
            os.makedirs(parent, exist_ok=True)
        file_handler = logging.FileHandler(
            log_file, encoding="utf-8", errors="backslashreplace"
        )
        file_handler.setFormatter(_ControlFormatter(fmt, datefmt))
        logger.addHandler(file_handler)
    if not logger.handlers:
        # --no-console with no --log-file silences everything; a NullHandler
        # keeps logging calls cheap and warning-free rather than erroring.
        logger.addHandler(logging.NullHandler())
    return logger


def progress(iterable, label, *, total=None, interval=2.0, logger=None):
    """Yield from ``iterable``, logging progress at most once per ``interval``.

    ``label`` names the work (e.g. ``"crawl"``). ``total`` gives the denominator;
    when it is omitted it is taken from ``len(iterable)`` for a sized iterable
    (a list) and left absent for a generator, so a caller never has to size an
    arbitrary iterable itself. A final line always reports the count, so a loop
    shorter than ``interval`` still logs once.
    """
    if total is None and isinstance(iterable, Sized):
        total = len(iterable)
    log = logger or logging.getLogger(LOGGER_NAME)
    done = 0
    last = time.monotonic()
    for item in iterable:
        yield item
        done += 1
        now = time.monotonic()
        if now - last >= interval:
            log.info("%s: %s", label, _fraction(done, total))
            last = now
    log.info("%s: %s done", label, _fraction(done, total))


def _fraction(done, total):
    return f"{done}/{total}" if total is not None else str(done)
