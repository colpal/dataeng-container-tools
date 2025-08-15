"""Mock file to test CustomCommandLineArgument."""

from dataeng_container_tools import CommandLineArguments, CustomCommandLineArgument


def main() -> None:
    """Setup CommandLineArguments."""
    some_arg = CustomCommandLineArgument(
        name="some_arg",
        nargs="+",
        type=str,
        required=True,
    )

    some_arg2 = CustomCommandLineArgument(
        name="some_arg2",
        type=int,
        required=False,
    )

    CommandLineArguments(custom_args=[some_arg, some_arg2])


main()
