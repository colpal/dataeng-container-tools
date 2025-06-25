"""Mock file to test secret_locations CommandLineArgument."""

import logging
import sys

from dataeng_container_tools import CommandLineArguments, CommandLineArgumentType

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    """Setup CommandLineArguments with secret_locations."""
    cla = CommandLineArguments(
        secret_locations=CommandLineArgumentType.REQUIRED,
        parse_known_args=False,
    )

    # Log the secret locations to verify they were parsed correctly
    args = cla.get_arguments()
    logger.info("Secret locations: %s", args.secret_locations)


if __name__ == "__main__":
    main()
