"""Mock file to test identifying_tags CommandLineArgument."""

import logging
import os
import sys

from dataeng_container_tools import CommandLineArguments, CommandLineArgumentType

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    """Setup CommandLineArguments with identifying tags."""
    cla = CommandLineArguments(
        identifying_tags=CommandLineArgumentType.REQUIRED,
        parse_known_args=False,
    )

    # Log the environment variables to verify they were set correctly
    args = cla.get_arguments()
    logger.info("DAG_ID: %s (env: %s)", args.dag_id, os.getenv("DAG_ID", "NOT_SET"))
    logger.info("RUN_ID: %s (env: %s)", args.run_id, os.getenv("RUN_ID", "NOT_SET"))
    logger.info("NAMESPACE: %s (env: %s)", args.namespace, os.getenv("NAMESPACE", "NOT_SET"))
    logger.info("POD_NAME: %s (env: %s)", args.pod_name, os.getenv("POD_NAME", "NOT_SET"))


if __name__ == "__main__":
    main()
