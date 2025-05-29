"""Mock file to test required, optional, and unused CommandLineArgumentType."""

from dataeng_container_tools import CommandLineArguments, CommandLineArgumentType


def main() -> None:
    """Setup CommandLineArguments."""
    CommandLineArguments(
        secret_locations=CommandLineArgumentType.REQUIRED,
        input_files=CommandLineArgumentType.OPTIONAL,
        output_files=CommandLineArgumentType.UNUSED,
        parse_known_args=False,  # Allow testing whether output_files is non-existant
    )


main()
