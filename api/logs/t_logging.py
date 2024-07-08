import sys
import logging

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)

# Create handlers for logging to the standard output and a file
stdoutHandler = logging.StreamHandler(stream=sys.stdout)
errHandler = logging.FileHandler("C:\\Users\\Translab\\logs\\error.log")


# Set the log levels on the handlers
stdoutHandler.setLevel(logging.DEBUG)
errHandler.setLevel(logging.DEBUG)

# Create a log format using Log Record attributes
fmt = logging.Formatter("%(name)s: %(asctime)s | %(levelname)s | %(filename)s:%(lineno)s | %(process)d >>> %(message)s")

# Set the log format on each handler
stdoutHandler.setFormatter(fmt)
errHandler.setFormatter(fmt)

# Add each handler to the Logger object
logger.addHandler(stdoutHandler)
logger.addHandler(errHandler)
