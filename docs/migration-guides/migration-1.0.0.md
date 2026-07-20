# Migration Guide: 0.6.4 to 1.0.0

This guide covers the important changes when migrating a container from DataEng Container Tools `v0.6.4` to `v1.0.0`.

`v1.0.0` is a large, breaking release. The package was restructured, most classes were renamed to `PascalCase`, several modules were removed, and the two most used modules (CLA and GCS) changed their public APIs significantly.

## Highlights

- The importable package was renamed from `dataEng_container_tools` to `dataeng_container_tools`.
- All public classes are now importable directly from the top-level package (e.g. `from dataeng_container_tools import GCSFileIO`).
- Classes were renamed from `snake_case` to `PascalCase`.
- Secret handling moved to a dedicated `SecretManager` / `SecretLocations` system.
- The GCS file operations collapsed from eight methods into `download` and `upload`.
- The `BQ`, `simple_setup`, and `exceptions` modules were removed.
- Python 3.7-3.9 support was dropped; `v1.0.0` requires Python 3.10 or higher.
- New optional modules were added: [Snowflake](../modules/snowflake.md) and [Download](../modules/downloader.md).

## Imports and Renames

The importable package name changed, and everything is now available from the top level.

```python
# Before (v0.6.4)
from dataEng_container_tools.cla import (
    command_line_arguments,
    command_line_argument_type,
)
from dataEng_container_tools.gcs import gcs_file_io

# After (v1.0.0)
from dataeng_container_tools import (
    CommandLineArguments,
    CommandLineArgumentType,
    GCSFileIO,
)
```

The following files, classes, and functions have been renamed or relocated:

| Old (`v0.6.4`) | New (`v1.0.0`) |
| --- | --- |
| `cla.custom_command_line_argument` | `CustomCommandLineArgument` |
| `cla.command_line_argument_type` | `CommandLineArgumentType` |
| `cla.command_line_secret` | `secrets_manager.SecretLocations` |
| `cla.command_line_arguments` | `CommandLineArguments` |
| `cla.command_line_arguments.get_secret_locations` | `secrets_manager.SecretLocations` |
| `cla.command_line_arguments.get_secrets` | `secrets_manager.SecretManager.secrets` |
| `gcs.gcs_file_io` | `modules.gcs.GCSFileIO` |
| `gcs.gcs_file_io.__get_parts` | `modules.gcs.GCSUriUtils.get_components` |
| `db.Db` | `modules.datastore.Datastore` |
| `safe_stdout.safe_stdout` | `safe_textio.SafeTextIO` |
| `safe_stdout.safe_stdout.add_words` | `safe_textio.SafeTextIO.add_words` (classmethod) |
| `safe_stdout.setup_default_stdout` | `safe_textio.setup_default_stdio` |
| `safe_stdout.setup_stdout` | `secrets_manager.SecretManager.process_secret_folder` |

## Removed

The following were removed entirely in `v1.0.0`:

- Python 3.7-3.9 support.
- The `BQ` (BigQuery) module and `bq.py`. See [BigQuery Alternative](#bigquery-alternative).
- The `simple_setup` module and `simple_setup.py`.
- The `exceptions` module and `exceptions.py`.
- `CommandLineArguments.check_args` (was a no-op).
- `CommandLineArguments.get_pandas_kwargs`, the `--input_pandas_kwargs` / `--output_pandas_kwargs` args, and their `input_pandas_kwargs` / `output_pandas_kwargs` init parameters.
- The `--input_dtypes` arg and `get_input_dtypes`, along with the `input_dtypes` and `default_file_type` init parameters.
- The `--input_delimiters` / `--output_delimiters` args.
- The GCS `local=True` local-filesystem behavior (the flag still exists but now targets an emulator, see [GCS](#gcs)).
- `pickle` / `pkl` support in GCS (removed for security reasons).

## Command Line Arguments (CLA)

`command_line_arguments` became [`CommandLineArguments`](../modules/command-line.md). The most impactful changes:

- Preset arguments are now keyword only (only `custom_args` is positional).
- `custom_inputs` was renamed to `custom_args`.
- `parse_known_args` now defaults to `True`, so unknown extra arguments are ignored instead of raising.
- `CommandLineArgumentType` gained an explicit `UNUSED` member (the default), replacing the implicit `None`.
- Secret retrieval helpers were removed in favor of [`SecretLocations`](#secrets-and-safe-output).

```python
# Before (v0.6.4)
from dataEng_container_tools.cla import command_line_arguments, command_line_argument_type

my_inputs = command_line_arguments(
    secret_locations=command_line_argument_type.OPTIONAL,
    input_files=command_line_argument_type.REQUIRED,
    output_files=command_line_argument_type.REQUIRED,
)
input_uris = my_inputs.get_input_uris()
output_uris = my_inputs.get_output_uris()
secret_locations = my_inputs.get_secret_locations()

# After (v1.0.0)
from dataeng_container_tools import (
    CommandLineArguments,
    CommandLineArgumentType,
    SecretLocations,
)

CommandLineArguments(
    secret_locations=CommandLineArgumentType.OPTIONAL,
    input_files=CommandLineArgumentType.REQUIRED,
    output_files=CommandLineArgumentType.REQUIRED,
)
cla = CommandLineArguments()  # Singleton, returns the same instance
input_uris = cla.get_input_uris()
output_uris = cla.get_output_uris()
secret_locations = SecretLocations()  # Populated automatically from --secret_locations
```

!!! note
    `CommandLineArguments` is now a singleton. Constructing it once configures the parser; subsequent calls return the same instance.

### Custom Arguments

`CustomCommandLineArgument` renamed two of its parameters to align with `argparse`:

- `data_type` -> `type`
- `help_message` -> `help`

```python
# Before (v0.6.4)
from dataEng_container_tools.cla import custom_command_line_argument

arg = custom_command_line_argument(
    name="batch_size",
    data_type=int,
    default=32,
    help_message="Items per batch",
)

# After (v1.0.0)
from dataeng_container_tools import CustomCommandLineArgument

arg = CustomCommandLineArgument(
    name="batch_size",
    type=int,
    default=32,
    help="Items per batch",
)
```

### Running Local

The `running_local` argument was removed. Use the [`IS_LOCAL`](../modules/container-utils.md) constant, which detects the container environment automatically.

```python
# Before (v0.6.4): passed running_local into command_line_arguments / gcs_file_io

# After (v1.0.0)
from dataeng_container_tools import IS_LOCAL

if IS_LOCAL:
    ...
```

## GCS

`gcs_file_io` became [`GCSFileIO`](../modules/gcs-operations.md), and the eight download/upload methods were consolidated into two.

| Old (`v0.6.4`) | New (`v1.0.0`) |
| --- | --- |
| `download_file_to_object`, `download_files_to_objects`, `download_file_to_disk`, `download_files_to_disk` | `download` |
| `upload_file_from_object`, `upload_files_from_objects`, `upload_file_from_disk`, `upload_files_from_disk` | `upload` |

Other changes:

- `gcs_secret_location` is now optional. If omitted, credentials are resolved from [`SecretLocations`](#secrets-and-safe-output) and then the default path `/vault/secrets/gcp-sa-storage.json`.
- `download` returns a `dict` keyed by the object path (`{"bucket/path/file.csv": DataFrame}`), rather than a bare object or list.
- Whether an operation targets an object or a local file is decided by the shape of the argument you pass (a URI string versus a `(source, destination)` tuple).
- `pkl` / `pickle` is no longer supported.
- `local=True` no longer reads/writes the local filesystem directly; it now points the client at a GCS emulator. Use [`pathlib`](https://docs.python.org/3/library/pathlib.html) for plain local files.

### Downloading

```python
# Before (v0.6.4)
from dataEng_container_tools.gcs import gcs_file_io

file_io = gcs_file_io(gcs_secret_location=secret_locations.GCS)
df = file_io.download_file_to_object("gs://my-bucket/data.csv")

# After (v1.0.0)
from dataeng_container_tools import GCSFileIO

gcs = GCSFileIO()  # secret resolved automatically
df, = gcs.download("gs://my-bucket/data.csv").values()

# Download straight to a local file with a (source, destination) tuple
gcs.download(("gs://my-bucket/data.csv", "./data.csv"))
```

### Uploading

```python
# Before (v0.6.4)
result = file_io.upload_file_from_object(
    gcs_uri="gs://my-bucket/out.csv",
    object_to_upload=df,
)

# After (v1.0.0): pass a (source, destination) tuple
gcs.upload((df, "gs://my-bucket/out.csv"))

# Upload a local file by passing a path as the source
gcs.upload(("./out.csv", "gs://my-bucket/out.csv"))
```

Both `download` and `upload` accept a list of tuples for batch operations. See [GCS Operations](../modules/gcs-operations.md) for globs, metadata, batching, and file-format details.

## Secrets and Safe Output

Secret discovery, storage, and output censoring were split into [`SecretManager`, `SecretLocations`, and `SafeTextIO`](../modules/secrets-handling.md).

### Secret Locations

`command_line_secret` (with its `GCS` / `BQ` attributes) became the [`SecretLocations`](../modules/secrets-handling.md) singleton, which exposes `GCS`, `SF`, and `DS` (note `BQ` is gone). It is populated automatically when `CommandLineArguments` parses `--secret_locations`.

```python
# Before (v0.6.4)
secret_locations = my_inputs.get_secret_locations()
gcs_secret = secret_locations.GCS

# After (v1.0.0)
from dataeng_container_tools import SecretLocations

gcs_secret = SecretLocations().GCS
sf_secret = SecretLocations().SF
custom_secret = SecretLocations()["CUSTOM"]  # keys added via --secret_locations
```

### Parsing Secrets

`command_line_arguments.get_secrets` and `safe_stdout.setup_stdout` were replaced by `SecretManager`. Secrets in `/vault/secrets/` are parsed automatically on import; the parsed values are available on `SecretManager.secrets`.

```python
# Before (v0.6.4)
secrets = my_inputs.get_secrets()

# After (v1.0.0)
from dataeng_container_tools import SecretManager

all_secrets = SecretManager.secrets  # dict keyed by file path
one_secret = SecretManager.parse_secret("/vault/secrets/api_key")
```

### Safe Output

`safe_stdout` became [`SafeTextIO`](../modules/secrets-handling.md). `sys.stdout` and `sys.stderr` are wrapped automatically on import via `setup_default_stdio` (formerly `setup_default_stdout`). `add_words` is now a classmethod that applies to all instances, and the constructor requires the `textio` stream it wraps.

```python
# Before (v0.6.4)
import sys
sys.stdout.add_words(["my_secret"])

# After (v1.0.0)
from dataeng_container_tools import SafeTextIO

SafeTextIO.add_words(["my_secret"])
```

!!! note
    Secrets parsed by `SecretManager` are added to `SafeTextIO` automatically, so you rarely need to call `add_words` yourself.

## Datastore

`db.Db` became [`Datastore`](../modules/datastore-operations.md) and requires the `datastore` extra. The Datastore client is now created and held internally, so you no longer construct or pass a `client` around. `get_secrets` and `get_data_store_client` were removed.

```python
# Before (v0.6.4)
from dataEng_container_tools.db import Db

db = Db(task_kind="MyTask")
client = db.get_data_store_client("/path/to/credentials.json")
entries = db.get_task_entry(client, filter_map, kind="MyTask")
db.handle_task(client, params)

# After (v1.0.0)
from dataeng_container_tools import Datastore

ds = Datastore(task_kind="MyTask", gcp_secret_location="/path/to/credentials.json")
entries = ds.get_task_entry(filter_map, kind="MyTask")
ds.handle_task(params)
```

## BigQuery Alternative

The `BQ` module was removed. There is no drop in replacement in `v1.0.0`. Options:

- Use the [`google-cloud-bigquery`](https://cloud.google.com/python/docs/reference/bigquery/latest) client directly, authenticating with a secret resolved via `SecretLocations`.

## Full Example

A minimal end-to-end container, before and after.

```python
# Before (v0.6.4)
from dataEng_container_tools.cla import command_line_arguments, command_line_argument_type
from dataEng_container_tools.gcs import gcs_file_io

my_inputs = command_line_arguments(
    secret_locations=command_line_argument_type.OPTIONAL,
    input_files=command_line_argument_type.REQUIRED,
    output_files=command_line_argument_type.REQUIRED,
)

input_uris = my_inputs.get_input_uris()
output_uris = my_inputs.get_output_uris()
secret_locations = my_inputs.get_secret_locations()

file_io = gcs_file_io(gcs_secret_location=secret_locations.GCS)
df = file_io.download_file_to_object(input_uris[0])

# Edit df here.

file_io.upload_file_from_object(gcs_uri=output_uris[0], object_to_upload=df)
```

```python
# After (v1.0.0)
from dataeng_container_tools import (
    CommandLineArguments,
    CommandLineArgumentType,
    GCSFileIO,
)

CommandLineArguments(
    secret_locations=CommandLineArgumentType.OPTIONAL,
    input_files=CommandLineArgumentType.REQUIRED,
    output_files=CommandLineArgumentType.REQUIRED,
)
cla = CommandLineArguments()

input_uris = cla.get_input_uris()
output_uris = cla.get_output_uris()

gcs = GCSFileIO()  # credentials resolved automatically from SecretLocations
df, = gcs.download(input_uris[0]).values()

# Edit df here.

gcs.upload((df, output_uris[0]))
```

For deeper details on any module, see the [Modules](../modules/index.md) section.
