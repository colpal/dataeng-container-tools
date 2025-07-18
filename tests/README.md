# Testing

This project uses Pytest for basic testing and coverage/

To run the test suite, use the following commands (assuming cwd is at the root of the repository):

```bash
pytest --cov=dataeng_container_tools tests/
```

To generate an HTML coverage report, run:

```bash
pytest --cov=dataeng_container_tools --cov-report=html tests/
```

The coverage report will be generated in the `htmlcov/` directory.
