import logging

import beartype


@beartype.beartype
def _configure_logging(*, silent: bool) -> None:
    """Configure root logger level based on the *silent* flag.

    When *silent* is ``False`` (the default), the root logger is configured
    at ``INFO`` level so all informational messages are visible.  When
    *silent* is ``True``, the root logger is set to ``WARNING``, suppressing
    purely informational output.
    """
    level = logging.WARNING if silent else logging.INFO
    logging.basicConfig(level=level, format="%(message)s")
