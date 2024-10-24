from pulse_telemetry.logging import get_logger


def test_logger_inherits_root_configuration():
    logger = get_logger()
    assert not logger.handlers, "The logger should not have its own handlers"
    assert logger.propagate, "The logger should propagate to the root logger"
    assert logger.hasHandlers(), "The logger should be able to access the root logger's handler"
