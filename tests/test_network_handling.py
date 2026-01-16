import unittest
from unittest.mock import MagicMock, patch

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dwdown.utils import SessionHandler


class TestSessionHandler(unittest.TestCase):
    def setUp(self):
        # Setup any state or objects before each test method is called
        self.default_retries = 5
        self.default_backoff_factor = 2.0
        self.default_status_forcelist = (429, 500, 502, 503, 504)

    def test_init_default_values(self):
        # Test the default values for num_retries, backoff_factor, and status_forcelist
        handler = SessionHandler()
        self.assertEqual(handler._num_retries, self.default_retries)
        self.assertEqual(handler._backoff_factor, self.default_backoff_factor)
        self.assertEqual(handler._status_forcelist, self.default_status_forcelist)

    def test_init_custom_values(self):
        # Test custom values for num_retries, backoff_factor, and status_forcelist
        custom_retries = 3
        custom_backoff_factor = 1.5
        custom_status_forcelist = (404, 500)
        handler = SessionHandler(custom_retries, custom_backoff_factor, custom_status_forcelist)
        self.assertEqual(handler._num_retries, custom_retries)
        self.assertEqual(handler._backoff_factor, custom_backoff_factor)
        self.assertEqual(handler._status_forcelist, custom_status_forcelist)

    @patch('urllib3.util.retry.Retry')
    @patch('requests.adapters.HTTPAdapter')
    @patch('requests.Session')
    def test_create_session(self, mock_session, mock_http_adapter, mock_retry):
        # Test the _create_session method
        handler = SessionHandler()
        mock_retry_instance = mock_retry.return_value
        mock_http_adapter_instance = mock_http_adapter.return_value
        mock_session_instance = mock_session.return_value

        session = handler._create_session()

        mock_retry.assert_called_once_with(
            total=self.default_retries,
            backoff_factor=self.default_backoff_factor,
            status_forcelist=self.default_status_forcelist
        )
        mock_http_adapter.assert_called_once_with(max_retries=mock_retry_instance)
        mock_session_instance.mount.assert_called_once_with("https://", mock_http_adapter_instance)
        self.assertEqual(session, mock_session_instance)

    @patch('requests.Session', side_effect=Exception("Failed to create session"))
    def test_create_session_exception(self, mock_session):
        # Test the _create_session method when an exception occurs
        handler = SessionHandler()
        with self.assertRaises(RuntimeError) as context:
            handler._create_session()
        self.assertIn("Failed to create session", str(context.exception))

    @patch('dwdown.utils.network_handling.SessionHandler._create_session')
    def test_get_session(self, mock_create_session):
        # Test the get_session method
        handler = SessionHandler()
        mock_session_instance = MagicMock()
        mock_create_session.return_value = mock_session_instance

        session = handler.get_session()

        mock_create_session.assert_called_once()
        self.assertEqual(session, mock_session_instance)


if __name__ == '__main__':
    unittest.main()
