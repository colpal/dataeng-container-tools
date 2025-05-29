"""Unit tests for the safe_textio module."""

import io
import sys
import unittest
from pathlib import Path

from dataeng_container_tools import SafeTextIO, SecretManager, setup_default_stdio


class TestSafeTextIO(unittest.TestCase):
    """Tests for the SafeTextIO class and related functions."""

    # Remove this noqa when https://github.com/astral-sh/ruff/issues/17197 is fixed
    # ruff: noqa: SLF001

    # For editing mangled functions
    # ruff: noqa: B009, B010

    def setUp(self) -> None:
        """Set up test environment."""
        # Store original functions/values
        self.orig_get_word_variants = getattr(SafeTextIO, "_SafeTextIO__get_word_variants")

        # Set test secrets folder
        self.test_secrets_folder = Path(__file__).parent / "mock_files" / "vault" / "secrets"

    def test_safe_textio_init(self) -> None:
        """Test SafeTextIO initialization."""
        # Ensure bad_words is empty before this test
        SafeTextIO._bad_words = set()

        # Test this without variants for fixed length size
        setattr(SafeTextIO, "_SafeTextIO__get_word_variants", lambda word: {word})

        # Test init with explicit bad words
        test_io = io.StringIO()
        bad_words = ["secret1", "password123"]
        SafeTextIO(textio=test_io, bad_words=bad_words)

        # Restore originals
        setattr(SafeTextIO, "_SafeTextIO__get_word_variants", self.orig_get_word_variants)

        # Verify bad_words are correctly added to the class variable
        assert len(SafeTextIO._bad_words) == 2, (
            f"Expected 2 bad words, got {len(SafeTextIO._bad_words)}. Bad words: {SafeTextIO._bad_words}"
        )

    def test_write_with_secrets(self) -> None:
        """Test that secrets are censored in output."""
        # Ensure bad_words is empty before this test
        SafeTextIO._bad_words = set()

        test_io = io.StringIO()
        safe_io = SafeTextIO(textio=test_io)
        SafeTextIO.add_words(["secret", "password"])

        # Test writing a message containing secrets
        safe_io.write("My secret password is hidden.")
        assert test_io.getvalue() == f"My {'*' * 6} {'*' * 8} is hidden.", (
            f"Expected censored output, got: '{test_io.getvalue()}'"
        )

    def test_write_without_secrets(self) -> None:
        """Test a message without secrets."""
        # Ensure bad_words is completely empty for this test
        SafeTextIO._bad_words = set()

        test_io = io.StringIO()
        safe_io = SafeTextIO(textio=test_io)
        safe_io.write("No secrets here.")
        assert test_io.getvalue() == "No secrets here.", f"Expected uncensored output, got: '{test_io.getvalue()}'"

    def test_add_words(self) -> None:
        """Test the add_words class method."""
        # Ensure bad_words is empty before this test
        SafeTextIO._bad_words = set()

        # Test adding a simple string
        SafeTextIO.add_words(["simple_secret"])
        assert "simple_secret" in SafeTextIO._bad_words, (
            f"Expected 'simple_secret' in bad_words. Current bad_words: {SafeTextIO._bad_words}"
        )

        # Test adding objects with __str__ method
        class TestObj:
            def __str__(self) -> str:
                return "object_secret"

        test_obj = TestObj()
        SafeTextIO.add_words([test_obj])
        assert "object_secret" in SafeTextIO._bad_words, (
            f"Expected 'object_secret' in bad_words. Current bad_words: {SafeTextIO._bad_words}"
        )

    def test_add_secrets_folder(self) -> None:
        """Test the add_secrets_folder function with actual files."""
        # Ensure bad_words is empty before this test
        SafeTextIO._bad_words = set()

        # Call the function to add secrets from the folder
        SecretManager.process_secret_folder(self.test_secrets_folder)

        # Verify secrets from api-key.json were added
        assert "1234567890abcdefghijklmnopqrstuvwxyz" in SafeTextIO._bad_words, (
            f"API key not found in bad_words. Current bad_words: {SafeTextIO._bad_words}"
        )
        assert "client_987654321" in SafeTextIO._bad_words, (
            f"Client ID not found in bad_words. Current bad_words: {SafeTextIO._bad_words}"
        )

        # Verify secrets from db_credentials.json were added
        assert "db_admin" in SafeTextIO._bad_words, (
            f"DB username not found in bad_words. Current bad_words: {SafeTextIO._bad_words}"
        )
        assert "supersecretpassword123!" in SafeTextIO._bad_words, (
            f"DB password not found in bad_words. Current bad_words: {SafeTextIO._bad_words}"
        )
        assert (
            "postgresql://db_admin:supersecretpassword123!@database.example.com:5432/production_db"
            in SafeTextIO._bad_words
        ), f"Connection string not found in bad_words. Current bad_words: {SafeTextIO._bad_words}"

    def test_setup_default_stdio(self) -> None:
        """Test setup_default_stdio function."""
        # Call the setup function
        setup_default_stdio()

        # Verify stdout and stderr are wrapped
        assert isinstance(sys.stdout, SafeTextIO), f"Expected sys.stdout to be SafeTextIO, got: {type(sys.stdout)}"
        assert isinstance(sys.stderr, SafeTextIO), f"Expected sys.stderr to be SafeTextIO, got: {type(sys.stderr)}"

    def test_stdout_stderr_censoring(self) -> None:
        """Test that both stdout and stderr censor secrets."""
        # Ensure bad_words is empty before this test
        SafeTextIO._bad_words = set()

        orig_stdout = sys.stdout
        orig_stderr = sys.stderr

        # Setup test streams
        test_stdout = io.StringIO()
        test_stderr = io.StringIO()

        # Setup SafeTextIO on those streams
        sys.stdout = SafeTextIO(textio=test_stdout)
        sys.stderr = SafeTextIO(textio=test_stderr)

        # Add a secret
        bad_word = "confidential"
        SafeTextIO.add_words([bad_word])

        # Write to both streams
        sys.stdout.write(f"This is {bad_word} information")
        sys.stderr.write(f"Error: {bad_word} data exposed")

        # Restore originals
        sys.stdout = orig_stdout
        sys.stderr = orig_stderr

        # Check censoring in both streams
        assert test_stdout.getvalue() == f"This is {'*' * len(bad_word)} information", (
            f"Expected censored stdout, got: '{test_stdout.getvalue()}'"
        )
        assert test_stderr.getvalue() == f"Error: {'*' * len(bad_word)} data exposed", (
            f"Expected censored stderr, got: '{test_stderr.getvalue()}'"
        )

    def test_multiple_instances(self) -> None:
        """Test that multiple instances of SafeTextIO share the same bad_words set."""
        # Ensure bad_words is empty before this test
        SafeTextIO._bad_words = set()

        test_io1 = io.StringIO()
        test_io2 = io.StringIO()

        safe_io1 = SafeTextIO(textio=test_io1)
        safe_io1.add_words(["shared_secret"])

        safe_io2 = SafeTextIO(textio=test_io2)

        # Both instances should censor the same word
        safe_io1.write("This is a shared_secret message.")
        safe_io2.write("Another shared_secret message.")

        assert test_io1.getvalue() == f"This is a {'*' * 13} message.", (
            f"Expected censored output in io1, got: '{test_io1.getvalue()}'"
        )
        assert test_io2.getvalue() == f"Another {'*' * 13} message.", (
            f"Expected censored output in io2, got: '{test_io2.getvalue()}'"
        )

    def test_parse_secret(self) -> None:
        """Test the parse_secret function with actual files."""
        # Ensure bad_words is empty before this test
        SafeTextIO._bad_words = set()

        # Use the api-key.json file that already exists
        test_path = self.test_secrets_folder / "api-key.json"

        # Call the parse_secret function
        SecretManager.parse_secret(test_path)

        # Check that the secrets were added in various formats
        assert "1234567890abcdefghijklmnopqrstuvwxyz" in SafeTextIO._bad_words, (
            f"API key not found in bad_words. Current bad_words: {SafeTextIO._bad_words}"
        )
        assert "client_987654321" in SafeTextIO._bad_words, (
            f"Client ID not found in bad_words. Current bad_words: {SafeTextIO._bad_words}"
        )
        assert "production" in SafeTextIO._bad_words, (
            f"Environment not found in bad_words. Current bad_words: {SafeTextIO._bad_words}"
        )

        # Check the JSON format versions too
        assert '"1234567890abcdefghijklmnopqrstuvwxyz"' in SafeTextIO._bad_words, (
            f"Quoted API key not found in bad_words. Current bad_words: {SafeTextIO._bad_words}"
        )
        assert '"client_987654321"' in SafeTextIO._bad_words, (
            f"Quoted client ID not found in bad_words. Current bad_words: {SafeTextIO._bad_words}"
        )

    def test_process_secret_folder(self) -> None:
        """Test adding secrets from actual files in the vault/secrets directory."""
        # Ensure bad_words is empty before this test
        SafeTextIO._bad_words = set()

        # Call add_secrets_folder
        SecretManager.process_secret_folder(self.test_secrets_folder)

        api_key = "1234567890abcdefghijklmnopqrstuvwxyz"
        db_pass = "supersecretpassword123!"  # noqa: S105

        # Check that the secrets from api-key.json were added
        assert api_key in SafeTextIO._bad_words, (
            f"API key not found in bad_words. Current bad_words: {SafeTextIO._bad_words}"
        )
        assert "client_987654321" in SafeTextIO._bad_words, (
            f"Client ID not found in bad_words. Current bad_words: {SafeTextIO._bad_words}"
        )

        # Check that the secrets from db_credentials.json were added
        assert "db_admin" in SafeTextIO._bad_words, (
            f"DB username not found in bad_words. Current bad_words: {SafeTextIO._bad_words}"
        )
        assert db_pass in SafeTextIO._bad_words, (
            f"DB password not found in bad_words. Current bad_words: {SafeTextIO._bad_words}"
        )

        test_io = io.StringIO()

        safe_io = SafeTextIO(textio=test_io)
        safe_io.write(f"API Key is {api_key} and password is {db_pass}")
        expected = f"API Key is {'*' * len(api_key)} and password is {'*' * len(db_pass)}"

        assert test_io.getvalue() == expected, (
            f"Expected secrets to be censored in output. Expected: '{expected}', got: '{test_io.getvalue()}'"
        )

    def test_no_secrets_message(self) -> None:
        """Test that messages without secrets pass through uncensored."""
        # Ensure bad_words is completely empty for this test
        SafeTextIO._bad_words = set()

        test_io = io.StringIO()

        safe_io = SafeTextIO(textio=test_io)
        safe_io.write("This message contains no secrets.")

        assert test_io.getvalue() == "This message contains no secrets.", (
            f"Expected uncensored output, got: '{test_io.getvalue()}'"
        )


if __name__ == "__main__":
    unittest.main()
