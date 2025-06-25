"""Mock file to test input and output files CommandLineArgument."""

import logging
import sys

from dataeng_container_tools import CommandLineArguments, CommandLineArgumentType

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    """Setup CommandLineArguments with input and output files."""
    cla = CommandLineArguments(
        input_files=CommandLineArgumentType.REQUIRED,
        output_files=CommandLineArgumentType.REQUIRED,
        parse_known_args=False,
    )

    # Log the URIs to verify they were built correctly
    input_uris = cla.get_input_uris()
    output_uris = cla.get_output_uris()

    logger.info("Input URIs: %s", input_uris)
    logger.info("Output URIs: %s", output_uris)


if __name__ == "__main__":
    main()
