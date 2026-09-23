import logging

import beartype

#: Third-party loggers whose informational output is noise to a command's user, such as
#: ``linkml_runtime`` announcing every schema import it resolves. Their warnings still show.
_QUIETED_LOGGERS = ("linkml_runtime",)


@beartype.beartype
def _configure_logging(*, silent: bool) -> None:
    """Configure root logger level based on the *silent* flag.

    When *silent* is ``False`` (the default), the root logger is configured
    at ``INFO`` level so all informational messages are visible.  When
    *silent* is ``True``, the root logger is set to ``WARNING``, suppressing
    purely informational output. Either way, the loggers in ``_QUIETED_LOGGERS``
    only report warnings and above.
    """
    level = logging.WARNING if silent else logging.INFO
    logging.basicConfig(level=level, format="%(message)s")
    for logger_name in _QUIETED_LOGGERS:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
