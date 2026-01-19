# Utils module
## SessionHandler

### Overview

The `SessionHandler` class handles HTTP sessions with retry logic, ensuring robust network requests.

### Constructor

```python
SessionHandler(
    num_retries: int = 5,
    backoff_factor: int | float = 2,
    status_forcelist: tuple | None = None
)
```

#### Parameters

- `num_retries` : `int`, default=`5`
  - The number of retries to attempt.
- `backoff_factor` : `int | float`, default=`2`
  - A backoff factor to apply between attempts.
- `status_forcelist` : `tuple | None`, default=`None`
  - A set of HTTP status codes that we should force a retry on. Defaults to (429, 500, 502, 503, 504).

### Methods

#### `get_session`

```python
get_session() -> Session
```

Returns the configured session with retry logic.

#### Returns

- `Session`
  - Configured session.


## ClientHandler

### Overview

The `ClientHandler` class handles the setup of a MinIO client for object storage interaction.

### Constructor

```python
ClientHandler(
    endpoint: str,
    access_key: str,
    secret_key: str,
    secure: bool = True
)
```

#### Parameters

- `endpoint` : `str`
  - The MinIO server endpoint.
- `access_key` : `str`
  - The access key for authentication.
- `secret_key` : `str`
  - The secret key for authentication.
- `secure` : `bool`, default=`True`
  - Whether to use HTTPS for the connection.

### Methods

#### `get_client`

```python
get_client() -> Minio
```

Returns the configured MinIO client.

#### Returns

- `Minio`
  - Configured MinIO client.
