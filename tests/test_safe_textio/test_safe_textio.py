"""Tests for the safe_textio module."""

import io
import sys
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from dataeng_container_tools import SafeTextIO, SecretManager
from dataeng_container_tools.safe_textio import setup_default_stdio


@pytest.fixture(autouse=True)
def cleanup_bad_words() -> Generator:
    """Automatically clean up bad words with each test."""
    SafeTextIO._bad_words = set()
    yield
    SafeTextIO._bad_words = set()


@pytest.fixture
def setup_test_environment() -> dict[str, Any]:
    """Set up test environment."""
    # Store original functions/values
    orig_get_word_variants = getattr(SafeTextIO, "_SafeTextIO__get_word_variants")  # noqa: B009

    # Set test secrets folder
    test_secrets_folder = Path(__file__).parent / "mock_files" / "vault" / "secrets"

    return {
        "orig_get_word_variants": orig_get_word_variants,
        "test_secrets_folder": test_secrets_folder,
    }


def test_safe_textio_init(setup_test_environment: dict[str, Any]) -> None:
    """Test SafeTextIO initialization."""
    # Test this without variants for fixed length size
    setattr(SafeTextIO, "_SafeTextIO__get_word_variants", lambda word: {word})  # noqa: B010

    # Test init with explicit bad words
    test_io = io.StringIO()
    bad_words = ["secret1", "password123"]
    SafeTextIO(textio=test_io, bad_words=bad_words)

    # Restore originals
    setattr(SafeTextIO, "_SafeTextIO__get_word_variants", setup_test_environment["orig_get_word_variants"])  # noqa: B010

    # Verify bad_words are correctly added to the class variable
    assert len(SafeTextIO._bad_words) == 2, (
        f"Expected 2 bad words, got {len(SafeTextIO._bad_words)}. Bad words: {SafeTextIO._bad_words}"
    )


def test_write_with_secrets() -> None:
    """Test that secrets are censored in output."""
    test_io = io.StringIO()
    safe_io = SafeTextIO(textio=test_io)
    SafeTextIO.add_words(["secret", "password"])

    # Test writing a message containing secrets
    safe_io.write("My secret password is hidden.")
    assert test_io.getvalue() == f"My {'*' * 6} {'*' * 8} is hidden.", (
        f"Expected censored output, got: '{test_io.getvalue()}'"
    )


def test_write_without_secrets() -> None:
    """Test a message without secrets."""
    test_io = io.StringIO()
    safe_io = SafeTextIO(textio=test_io)
    SafeTextIO.add_words(["secret", "password"])

    # Test writing a message without secrets
    safe_io.write("This is a normal message.")
    assert test_io.getvalue() == "This is a normal message.", f"Expected uncensored output, got: '{test_io.getvalue()}'"


def test_add_words() -> None:
    """Test the add_words class method."""
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


def test_add_secrets_folder(setup_test_environment: dict[str, Any]) -> None:
    """Test the add_secrets_folder function with actual files."""
    test_secrets_folder = setup_test_environment["test_secrets_folder"]

    # Call the function to add secrets from the folder
    SecretManager.process_secret_folder(test_secrets_folder)

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
        "postgresql://db_admin:supersecretpassword123!@database.example.com:5432/production_db" in SafeTextIO._bad_words
    ), f"Connection string not found in bad_words. Current bad_words: {SafeTextIO._bad_words}"


def test_setup_default_stdio() -> None:
    """Test setup_default_stdio function."""
    # Store original values
    orig_stdout = sys.stdout
    orig_stderr = sys.stderr

    try:
        # Call setup
        setup_default_stdio()

        # Verify that stdout and stderr are wrapped
        assert isinstance(sys.stdout, SafeTextIO), "Expected sys.stdout to be wrapped with SafeTextIO"
        assert isinstance(sys.stderr, SafeTextIO), "Expected sys.stderr to be wrapped with SafeTextIO"
    finally:
        # Restore original values
        sys.stdout = orig_stdout
        sys.stderr = orig_stderr


def test_stdout_stderr_censoring() -> None:
    """Test that both stdout and stderr censor secrets."""
    orig_stdout = sys.stdout
    orig_stderr = sys.stderr

    try:
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

        # Check censoring in both streams
        assert test_stdout.getvalue() == f"This is {'*' * len(bad_word)} information", (
            f"Expected censored stdout, got: '{test_stdout.getvalue()}'"
        )
        assert test_stderr.getvalue() == f"Error: {'*' * len(bad_word)} data exposed", (
            f"Expected censored stderr, got: '{test_stderr.getvalue()}'"
        )

    finally:
        # Restore original values
        sys.stdout = orig_stdout
        sys.stderr = orig_stderr


def test_multiple_instances() -> None:
    """Test that multiple instances of SafeTextIO share the same bad_words set."""
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


def test_parse_secret(setup_test_environment: dict[str, Any]) -> None:
    """Test the parse_secret function with actual files."""
    test_secrets_folder = setup_test_environment["test_secrets_folder"]

    # Use the api-key.json file that already exists
    test_path = test_secrets_folder / "api-key.json"

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


def test_process_secret_folder(setup_test_environment: dict[str, Any]) -> None:
    """Test adding secrets from actual files in the vault/secrets directory."""
    test_secrets_folder = setup_test_environment["test_secrets_folder"]

    # Call add_secrets_folder
    SecretManager.process_secret_folder(test_secrets_folder)

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


def test_no_secrets_message() -> None:
    """Test that messages without secrets pass through uncensored."""
    test_io = io.StringIO()
    safe_io = SafeTextIO(textio=test_io)

    # Add some secrets
    SafeTextIO.add_words(["secret", "password"])

    # Test a message without any secrets
    normal_message = "This is a completely normal message with no sensitive data."
    safe_io.write(normal_message)

    assert test_io.getvalue() == normal_message, f"Expected uncensored output, got: '{test_io.getvalue()}'"
