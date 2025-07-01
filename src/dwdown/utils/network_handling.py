from minio import Minio
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class SessionHandler:
    """
    A class to handle HTTP sessions with retry logic.

    Attributes:
        _num_retries (int): The number of retries to attempt.
        _backoff_factor (float): A backoff factor to apply between attempts.
        _status_forcelist (tuple): A set of HTTP status codes that we should force a retry on.
    """

    def __init__(
        self,
        num_retries: int = 5,
        backoff_factor: int | float = 2.0,
        status_forcelist: tuple | None = None
    ):
        """
        Initializes the SessionHandler with retry configuration.

        :param num_retries: The number of retries to attempt.
        :param backoff_factor: A backoff factor to apply between attempts.
        :param status_forcelist: A set of HTTP status codes that we should force a retry on.
        """
        self._num_retries = num_retries
        self._backoff_factor = backoff_factor
        self._status_forcelist = status_forcelist or (429, 500, 502, 503, 504)

        self._session = self._create_session()

    def _create_session(self) -> Session:
        """
        Creates and configures a new HTTP session with retry logic.

        :return: A configured HTTP session.
        :raises RuntimeError: If there is an issue setting up the session.
        """
        try:
            retry_strategy = Retry(
                total=self._num_retries,
                backoff_factor=self._backoff_factor,
                status_forcelist=self._status_forcelist)

            adapter = HTTPAdapter(max_retries=retry_strategy)
            session = Session()
            session.mount("https://", adapter)

            return session
        except Exception as e:
            raise RuntimeError(f"Failed to create session: {e}") from e

    def get_session(self) -> Session:
        """
        Returns the configured session.

        :return: Configured session.
        """
        return self._session


class ClientHandler:
    """
    A class to handle MinIO client setup.

    Attributes:
        _endpoint (str): The MinIO server endpoint.
        _access_key (str): The access key for authentication.
        _secret_key (str): The secret key for authentication.
        _secure (bool): Whether to use HTTPS for the connection.
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = True,
    ):
        """
        Initializes the ClientHandler with MinIO configuration.

        :param endpoint: The MinIO server endpoint.
        :param access_key: The access key for authentication.
        :param secret_key: The secret key for authentication.
        :param secure: Whether to use HTTPS for the connection.
        """
        self._endpoint = endpoint
        self._access_key = access_key
        self._secret_key = secret_key
        self._secure = secure

        self._client = self._create_client()

    def _create_client(self) -> Minio:
        """
        Creates and configures a new MinIO client.

        :return: A configured MinIO client.
        :raises RuntimeError: If there is an issue setting up the client.
        """
        try:
            client = Minio(
                endpoint=self._endpoint,
                access_key=self._access_key,
                secret_key=self._secret_key,
                secure=self._secure)
            return client
        except Exception as e:
            raise RuntimeError(f"Failed to create MinIO client: {e}") from e

    def get_client(self) -> Minio:
        """
        Returns the configured MinIO client.

        :return: Configured MinIO client.
        """
        return self._client
