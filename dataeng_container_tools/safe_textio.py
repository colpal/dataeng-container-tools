"""A modified version of standard I/O for censoring secrets.

Ensures that secrets are not accidentally printed using stdout or stderr. Has one
class SafeTextIO, two helper methods, add_secrets_folder and setup_default_stdio,
and one global variable default_secret_folder. On import it automatically searches
for secret files and adds their contents to the list of terms to censor. Also contains
global variables containing the default secret folder, the default GCS secret location,
and the list of secret files automatically found in the default secret folder.

Typical usage example:

    setup_default_stdio()
    print("Secret Information")    # prints "Secret Information"
    SafeTextIO.add_words('Secret')
    print("Secret Information)     # prints "******* Information"

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
    """A protocol that defines a __str__ method."""

    def __str__(self) -> str:
        """Return a string representation of the object."""
        ...


class SafeTextIO(TextIO):
    """Prints output with secrets removed.

    This class wraps any TextIO object so that when print or display
    calls are made this class is the one that processes them. The
    class maintains a list of 'bad_words' which are replaced with
    asterisks whenever someone tries to print them. By default this
    list is populated with the contents of any secret files that
    are in the default vault secrets folder.
    """

    _bad_words: ClassVar[set[str]] = set()
    _pattern_cache: ClassVar[tuple[re.Pattern, int]] = (re.compile(""), 0)  # Track int "version" of _bad_words

    def __init__(self, textio: TextIO, bad_words: Iterable[str | SupportsStr] = []) -> None:
        """Initialize safe_stdout with desired configuration.

        Args:
            textio: The original TextIO object to wrap.
            bad_words: An iterable of words to censor from output.

        """
        self.__old_textio = textio
        SafeTextIO.add_words(bad_words)

    def write(self, message: str | SupportsStr) -> int:
        """Output the desired message with secrets removed.

        Args:
          message: The message to output. Must either be a string or
            have a working __str__ method associated with it.

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
        """Add words to the list of words to censor from output.

        Args:
          bad_words: An iterable containing the words to
            add to the list of words to censor in output.

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
    """Censors the contents of JSONs found in the secrets folder from output.

    This function sets up both sys.stdout and sys.stderr with the SafeTextIO wrapper.
    This method should only be called once, and by design is called when this Python
    package is imported. To add values to the list of words to censor from output, use
    either add_secrets_folder() or SafeTextIO.add_words().
    """
    sys.stdout = SafeTextIO(textio=sys.stdout)
    sys.stderr = SafeTextIO(textio=sys.stderr)
