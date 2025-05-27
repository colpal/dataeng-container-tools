"""Tools for retrieving command line inputs.

Deals with receiving input from the command line. Has three
classes: `CustomCommandLineArgument`, `CommandLineArgumentType`,
and `CommandLineArguments`. `CommandLineArguments` contains most
of the functionality. `CommandLineArgumentType` is an enumeration.
`CustomCommandLineArgument` is a wrapper for `parser.add_argument()`.

Typical usage example:

    my_inputs = CommandLineArguments(secret_locations=CommandLineArgumentType.OPTIONAL,
                                    input_files=CommandLineArgumentType.REQUIRED,
                                    output_files=CommandLineArgumentType.REQUIRED)

    input_uris = my_inputs.get_input_uris()
    output_uris = my_inputs.get_output_uris()
    secret_locations = my_inputs.get_secret_locations()
    file_io = gcs_file_io(gcs_secret_location = secret_locations.GCS)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, final

from . import __version__
from .secrets_manager import SecretLocations

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger("Container Tools")


class CustomCommandLineArgument:
    """Class for creating custom command line arguments.

    Wrapper of `argparse.add_argument`. <br />
    Source: https://github.com/python/typeshed/blob/30b16c168d428f2690473e8d317c5eb240e7000e/stdlib/argparse.pyi

    This class is used for creating custom command line arguments. A
    list of these objects can be passed into `CommandLineArguments` which
    will add them as command line arguments, parse the inputs, and return the results.

    """

    def __init__(
        self,
        name: str,
        *,
        # str covers predefined actions ("store_true", "count", etc.)
        action: str | type[argparse.Action] = ...,
        # more precisely, Literal["?", "*", "+", "...", "A...", "==SUPPRESS=="]
        nargs: int | str | None = ...,  # None,
        const: Any = ...,  # noqa: ANN401
        default: Any = ...,  # noqa: ANN401
        type: argparse._ActionType = ...,  # noqa: A002
        choices: Iterable[argparse._T] | None = ...,
        required: bool = ...,
        help: str | None = ...,  # noqa: A002
        metavar: str | tuple[str, ...] | None = ...,
        dest: str | None = ...,
        version: str = ...,
        **kwargs: Any,  # noqa: ANN401
    ) -> None:
        """Initialize CustomCommandLineArgument with desired configuration.

        See: https://docs.python.org/3.9/library/argparse.html

        Args:
            name (str): Argument name.
            action (str | type[argparse.Action]): Indicates the basic type of action to be taken when this
                argument is encountered at the command line.
            nargs (int | str | None): Indicates the number of command-line arguments that should be consumed.
            const (Any): A constant value required by some action and nargs selections.
            default (Any): The value produced if the argument is absent from the command line and if it is
                absent from the namespace object.
            type (argparse._ActionType): The type to which the command-line argument should be converted.
            choices (Iterable[argparse._T] | None): A container of the allowable values for the argument.
            required (bool): Indicates whether or not the command-line option may be omitted (optionals only).
            help (str | None): A brief description of what the argument does.
            metavar (str | tuple[str, ...] | None): The name for the argument in usage messages.
            dest (str | None): The name of the attribute to be added to the object returned by parse_args().
            version (str): Version of the argument.
            kwargs (Any): Additional keyword arguments.

        """
        self.name = name
        self.action = action
        self.nargs = nargs
        self.const = const
        self.default = default
        self.type = type
        self.choices = choices
        self.required = required
        self.help = help
        self.metavar = metavar
        self.dest = dest
        self.version = version
        self.kwargs = kwargs

    def __str__(self) -> str:
        """Convert argument to a string."""
        attributes = [
            f"name: {self.name}",
            f"action: {self.action}",
            f"nargs: {self.nargs}",
            f"const: {self.const}",
            f"default: {self.default}",
            f"type: {self.type}",
            f"choices: {self.choices}",
            f"required: {self.required}",
            f"help: {self.help}",
            f"metavar: {self.metavar}",
            f"dest: {self.dest}",
            f"version: {self.version}",
            f"kwargs: {self.kwargs}",
        ]
        return ", ".join(attributes)


class CommandLineArgumentType(Enum):
    """Enumeration class for use with CommandLineArguments.

    Attributes:
        UNUSED: For when a command line argument should not be used.
        OPTIONAL: For when a command line argument should be optional.
        REQUIRED: For when a command line argument should be required.

    """

    UNUSED = None
    OPTIONAL = False
    REQUIRED = True


@final
class CommandLineArguments:
    """Creates, parses, and retrieves command line inputs.

    This class creates command line arguments that are typically
    used in Airflow containers. It will handle much of the backend
    boilerplate code involved with creating, parsing, and storing
    the relevant command line arguments using Python's argparse.
    Includes helper functions for using the command line inputs.
    """

    # Singleton instance
    _instance: ClassVar[CommandLineArguments | None] = None

    def __new__(cls, *_args: ..., **_kwargs: ...) -> CommandLineArguments:
        """Create a new instance of CommandLineArguments or return the existing one.

        Implements the singleton pattern to ensure only one instance exists.

        Returns:
            CommandLineArguments: The singleton instance
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        custom_inputs: list[CustomCommandLineArgument] | None = None,
        *,
        secret_locations: CommandLineArgumentType = CommandLineArgumentType.UNUSED,
        input_files: CommandLineArgumentType = CommandLineArgumentType.UNUSED,
        input_dtypes: CommandLineArgumentType = CommandLineArgumentType.UNUSED,
        output_files: CommandLineArgumentType = CommandLineArgumentType.UNUSED,
        identifying_tags: CommandLineArgumentType = CommandLineArgumentType.UNUSED,
        description: str | None = None,
        parser: argparse.ArgumentParser | None = None,
        parse_known_args: bool = True,
    ) -> None:
        """Initialize CommandLineArguments with desired configuration.

        Args:
            custom_inputs (list[CustomCommandLineArgument] | None): List of custom command line arguments.
            secret_locations (CommandLineArgumentType): Determines if secret locations are required, optional,
                or unused.
            input_files (CommandLineArgumentType): Determines if input files are required, optional, or unused.
            input_dtypes (CommandLineArgumentType): Determines if input data types are required, optional, or unused.
            output_files (CommandLineArgumentType): Determines if output files are required, optional, or unused.
            identifying_tags (CommandLineArgumentType): Determines if identifying tags are required, optional,
                or unused.
            description (str | None): Description for the command line parser.
            parser (argparse.ArgumentParser | None): Custom parser for command line arguments.
            parse_known_args (bool): Whether to parse known arguments only.

        """
        if custom_inputs is None:
            custom_inputs = []

        self.__custom_inputs = custom_inputs
        self.__secret_locations = secret_locations
        self.__input_files = input_files
        self.__input_dtypes = input_dtypes
        self.__output_files = output_files
        self.__identifying_tags = identifying_tags
        self.__description = description
        parser = parser if parser else argparse.ArgumentParser(description=description)

        self.__add_container_args(parser)

        if custom_inputs:
            for item in custom_inputs:
                arg_name = "--" + item.name
                arg_kwargs = {
                    attr: getattr(item, attr)
                    for attr in vars(item)
                    if attr not in ("name", "kwargs") and getattr(item, attr) is not ...
                }

                if item.kwargs is not ...:  # Add any additional kwargs
                    arg_kwargs.update(item.kwargs)
                parser.add_argument(arg_name, **arg_kwargs)

        try:
            if parse_known_args:
                self.__args, _ = parser.parse_known_args()  # Discard extra args
            else:
                self.__args = parser.parse_args()
        except Exception:
            logger.exception(
                "ARGUMENT ERROR: Reference the dataEng_container_tools README at "
                "https://github.com/colpal/dataEng-container-tools/blob/v%(version)s/README.md "
                "for examples of new updates from v%(version)s.",
                extra={"version": __version__},
            )
            raise
        logger.info("CLA Input: %s", self)

        # Update Secret Locations with args
        if self.__secret_locations.value is not None:
            SecretLocations().update(new_secret_locations=self.__args.secret_locations, set_attr=True)

        # Update env variables with args
        if identifying_tags.value is not None:
            os.environ["DAG_ID"] = self.__args.dag_id
            os.environ["RUN_ID"] = self.__args.run_id
            os.environ["NAMESPACE"] = self.__args.namespace
            os.environ["POD_NAME"] = self.__args.pod_name

    def __add_container_args(self, parser: argparse.ArgumentParser) -> None:
        if self.__input_files.value is not None:
            parser.add_argument(
                "--input_bucket_names",
                type=str,
                required=self.__input_files.value,
                nargs="+",
                help="GCS Buckets to read from.",
            )
            parser.add_argument(
                "--input_paths",
                type=str,
                required=self.__input_files.value,
                nargs="+",
                help="GCS folders in bucket to read file from.",
            )
            parser.add_argument(
                "--input_filenames",
                type=str,
                required=self.__input_files.value,
                nargs="+",
                help="Filenames to read file from.",
            )
            parser.add_argument(
                "--input_delimiters",
                type=str,
                required=False,
                nargs="+",
                help="Delimiters for input files",
            )

            if self.__input_dtypes.value is not None:
                parser.add_argument(
                    "--input_dtypes",
                    type=json.loads,
                    required=self.__input_dtypes.value,
                    nargs="+",
                    help="JSON dictionaries of (column: type) pairs to cast columns to",
                )

        if self.__output_files.value is not None:
            parser.add_argument(
                "--output_bucket_names",
                type=str,
                required=self.__output_files.value,
                nargs="+",
                help="GCS Bucket to write to.",
            )
            parser.add_argument(
                "--output_paths",
                type=str,
                required=self.__output_files.value,
                nargs="+",
                help="GCS folder in bucket to write file to.",
            )
            parser.add_argument(
                "--output_filenames",
                type=str,
                required=self.__output_files.value,
                nargs="+",
                help="Filename to write file to.",
            )
            parser.add_argument(
                "--output_delimiters",
                type=str,
                required=False,
                nargs="+",
                help="Delimiters for output files",
            )

        if self.__secret_locations.value is not None:
            parser.add_argument(
                "--secret_locations",
                type=json.loads,
                required=self.__secret_locations.value,
                default=SecretLocations(),
                help="Dictionary of the locations of secrets injected by Vault. Default: '"
                + str(SecretLocations())
                + "'.",
            )

        if self.__identifying_tags.value is not None:
            parser.add_argument(
                "--dag_id",
                type=str,
                required=self.__identifying_tags.value,
                default=os.getenv("DAG_ID", ""),
                help="The DAG ID",
            )
            parser.add_argument(
                "--run_id",
                type=str,
                required=self.__identifying_tags.value,
                default=os.getenv("RUN_ID", ""),
                help="The run ID",
            )
            parser.add_argument(
                "--namespace",
                type=str,
                required=self.__identifying_tags.value,
                default=os.getenv("NAMESPACE", ""),
                help="The namespace",
            )
            parser.add_argument(
                "--pod_name",
                type=str,
                required=self.__identifying_tags.value,
                default=os.getenv("POD_NAME", ""),
                help="The pod name",
            )

    def __str__(self) -> str:
        """Print the string value of the argparse args."""
        return self.__args.__str__()

    def get_arguments(self) -> argparse.Namespace:
        """Retrieve the arguments passed in through the command line.

        Returns:
            argparse.Namespace: A Namespace object with all of the command line arguments.

        """
        return self.__args

    def get_input_dtypes(self) -> ...:
        """Retrieve the input dtypes passed in through the command line.

        Returns:
            None if dtypes were not asked for in initialization or the loaded JSON
            object passed to input_dtypes through the command line otherwise.

        """
        return self.__args.input_dtypes

    def get_input_uris(self) -> list[str]:
        """Retrieve the input URIs passed in through the command line.

        Returns:
            list[str]: A list of all input URIs passed in through the command line. URIs
            are of the format 'gs://bucket_name/input_path/filename'.

        """
        if not self.__input_files:
            return []
        constant_bucket = False
        bucket_name = ""
        output: list[str] = []
        if len(self.__args.input_bucket_names) == 1:
            constant_bucket = True
            bucket_name = self.__args.input_bucket_names[0]
        for pos, filename in enumerate(self.__args.input_filenames):
            if not constant_bucket:
                bucket_name = self.__args.input_bucket_names[pos]
            prefix = r"gs://"
            uri_body = (
                f"{bucket_name}/{self.__args.input_paths[pos]}/{filename}".replace("/ /", "/")
                .replace("/./", "/")
                .replace("//", "/")
            )
            output.append(prefix + uri_body)
        return output

    def get_output_uris(self) -> list[str]:
        """Retrieve the output URIs passed in through the command line.

        Returns:
            list[str]: A list of all output URIs passed in through the command line. URIs
            are of the format 'gs://bucket_name/output_path/filename'.

        """
        if not self.__output_files:
            return []
        constant_bucket = False
        bucket_name = ""
        output: list[str] = []
        if len(self.__args.output_bucket_names) == 1:
            constant_bucket = True
            bucket_name = self.__args.output_bucket_names[0]
        for pos, filename in enumerate(self.__args.output_filenames):
            if not constant_bucket:
                bucket_name = self.__args.output_bucket_names[pos]
            prefix = r"gs://"
            uri_body = (
                f"{bucket_name}/{self.__args.output_paths[pos]}/{filename}".replace("/ /", "/")
                .replace("/./", "/")
                .replace("//", "/")
            )
            output.append(prefix + uri_body)
        return output
