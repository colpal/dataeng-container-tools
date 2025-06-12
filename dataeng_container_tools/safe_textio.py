"""A modified version of standard I/O for censoring secrets.

Ensures that secrets are not accidentally printed using stdout or stderr. Has one
class SafeTextIO, two helper methods, add_secrets_folder and setup_default_stdio,
and one global variable default_secret_folder. On import it automatically searches
for secret files and adds their contents to the list of terms to censor. Also contains
global variables containing the default secret folder, the default GCS secret location,
and the list of secret files automatically found in the default secret folder.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from typing import TYPE_CHECKING, ClassVar, Protocol, TextIO

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger("Container Tools")


class SupportsStr(Protocol):
    """Protocol for objects that support the `__str__` method.

    This protocol ensures that an object can be converted to a string
    representation, which is essential for classes like `SafeTextIO`
    that need to process various types of input for output.
    """

    def __str__(self) -> str:
        """Returns the string representation of the object.

        Returns:
            The string representation of the object.
        """
        ...


class SafeTextIO(TextIO):
    """A TextIO wrapper that censors sensitive information from output.

    This class wraps an existing TextIO stream (like `sys.stdout` or `sys.stderr`)
    to intercept and sanitize any text written to it. It maintains a list of
    "bad words" (secrets or sensitive data) which are replaced with asterisks
    before being printed. This helps prevent accidental leakage of secrets in logs
    or console outputs.

    The list of bad words can be populated automatically from secret files or
    manually by adding specific strings.

    Attributes:
        _bad_words (ClassVar[set[str]]): A set of strings to be censored.
        _pattern_cache (ClassVar[tuple[re.Pattern, int]]): Cache for the compiled
            regex pattern used for censoring, along with a version number to
            track changes to `_bad_words`.

    Examples:
        Using with `io.StringIO`:
            >>> import io
            >>> string_io = io.StringIO()
            >>> safe_io = SafeTextIO(string_io)
            >>> SafeTextIO.add_words(["confidential_data"])
            >>> safe_io.write("This message contains confidential_data.")
            >>> print(string_io.getvalue())
            This message contains *****************.
            >>> string_io.close()

        Using with an open file:
            >>> import os
            >>> file_path = "temp_test_file.txt"
            >>> with open(file_path, "w") as f:
            ...     safe_file_io = SafeTextIO(f)
            ...     SafeTextIO.add_words(["file_secret"])
            ...     safe_file_io.write("Secret in file: file_secret.")
            >>> with open(file_path, "r") as f:
            ...     content = f.read()
            >>> print(content)
            Secret in file: *************.
            >>> os.remove(file_path) # Clean up
    """

    _bad_words: ClassVar[set[str]] = set()
    _pattern_cache: ClassVar[tuple[re.Pattern, int]] = (re.compile(""), 0)  # Track int "version" of _bad_words

    def __init__(self, textio: TextIO, bad_words: Iterable[str | SupportsStr] = []) -> None:
        """Initializes a SafeTextIO instance.

        Args:
            textio: The TextIO object (e.g., `sys.stdout`, `sys.stderr`)
                to wrap and sanitize.
            bad_words: An initial iterable
                of words or objects convertible to strings that should be censored.
                Defaults to an empty list.
        """
        self.__old_textio = textio
        SafeTextIO.add_words(bad_words)

    def write(self, message: str | SupportsStr) -> int:
        """Writes the given message to the wrapped TextIO stream, censoring secrets.

        The message is first converted to a string. Then, any occurrences of
        "bad words" (secrets) are replaced with asterisks before writing to the
        underlying stream.

        Args:
            message: The message to write. Can be a string
                or any object that implements the `__str__` method.

        Returns:
            The number of characters written, as returned by the underlying
            TextIO object's write method.
        """
        # ruff: noqa: SLF001
        # Remove above noqa when https://github.com/astral-sh/ruff/issues/17197 is fixed

        message_str = str(message)

        # Skip processing if no bad words
        if not self.__class__._bad_words:
            return self.__old_textio.write(message_str)

        # Version will be the length, assume can only add words to _bad_words (no remove or modify)
        # Computing this is far easier than set comparison
        words_version = len(self.__class__._bad_words)

        # Cache hit check
        pattern, cached_version = self.__class__._pattern_cache
        if cached_version != words_version:  # Cache miss - rebuild the pattern
            # Sort by length descending to handle overlapping patterns correctly
            bad_words_sorted = sorted(self.__class__._bad_words, key=len, reverse=True)
            pattern_str = "|".join(re.escape(word) for word in bad_words_sorted)

            # Update cache
            pattern = re.compile(pattern_str)
            self.__class__._pattern_cache = (pattern, cached_version)

        # Replace all bad words in one pass
        censored_message = pattern.sub(lambda match: "*" * len(match.group(0)), message_str)

        return self.__old_textio.write(censored_message)

    @staticmethod
    def __get_word_variants(word: str) -> set[str]:
        return {
            word,
            json.dumps(word),  # JSON dump, e.g. "word"
            json.dumps(word).encode("unicode-escape").decode(),
            word.encode("unicode-escape").decode(),
        }

    @classmethod
    def add_words(cls, bad_words: Iterable[str | SupportsStr]) -> None:
        """Adds words to the class-level set of words to be censored.

        This method updates the `_bad_words` set, which is used by all
        `SafeTextIO` instances to identify and censor secrets. It also handles
        adding variants of the words (e.g., JSON-escaped versions) to
        improve censorship effectiveness.

        Args:
            bad_words: An iterable of words or
                objects convertible to strings to add to the censorship list.

        Examples:
            Adding words to censor:
                >>> setup_default_stdio() # Assuming stdout is now SafeTextIO
                >>> SafeTextIO.add_words(["new_secret_word"])
                >>> print("This is a new_secret_word.")
                This is a ***************.
                >>> SafeTextIO.add_words(["another_one", "sensitive_info"])
                >>> print("Testing another_one and sensitive_info.")
                Testing *********** and **************.

        """
        cls._bad_words.update(
            {
                variant
                for word in bad_words
                if str(word) not in cls._bad_words  # Skip if already censored
                for variant in cls.__get_word_variants(str(word))
            },
        )


def setup_default_stdio() -> None:
    """Replaces `sys.stdout` and `sys.stderr` with `SafeTextIO` wrappers.

    This function globally enables secret censoring for standard output and
    standard error streams. After calling this, any `print()` statements or
    direct writes to `sys.stdout` or `sys.stderr` will be processed by
    `SafeTextIO` to remove sensitive information.
    """
    sys.stdout = SafeTextIO(textio=sys.stdout)
    sys.stderr = SafeTextIO(textio=sys.stderr)


setup_default_stdio()
