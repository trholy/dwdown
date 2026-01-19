import logging
import unittest
from unittest.mock import MagicMock, patch

from dwdown.notify import Notifier


class TestNotifier(unittest.TestCase):
    def setUp(self):
        # Setup a basic logger for testing
        logging.basicConfig(level=logging.DEBUG)
        self.server_url = "http://example.com"
        self.token = "test_token"
        self.priority = 5
        self.secure = False
        self.notifier = Notifier(
            self.server_url, self.token, self.priority, self.secure)


    def test_init(self):
        # Test initialization with default values
        self.assertEqual(self.notifier.server_url, "http://example.com/message")
        self.assertEqual(self.notifier.token, "test_token")
        self.assertEqual(self.notifier.priority, 5)

        # Test initialization with secure=True
        notifier_secure = Notifier(self.server_url, self.token, secure=True)
        self.assertEqual(notifier_secure.server_url, "https://example.com/message")

        # Test initialization with no scheme in server_url
        notifier_no_scheme = Notifier("example.com", self.token)
        self.assertEqual(notifier_no_scheme.server_url, "http://example.com/message")


    def test_send_notification(self):
        # Mock the requests.post method
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.raise_for_status = MagicMock()

            # Test sending a simple message
            self.notifier.send_notification("Test message")
            mock_post.assert_called_once_with(
                "http://example.com/message",
                json={"title": "Script Status", "message": "Test message",
                      "priority": 5},
                headers={"X-Gotify-Key": "test_token"})
            mock_post.reset_mock()

            # Test sending a list of messages
            self.notifier.send_notification(
                ["Test message 1", "Test message 2"])
            mock_post.assert_called_once_with(
                "http://example.com/message",
                json={"title": "Script Status",
                      "message": "Test message 1\nTest message 2",
                      "priority": 5},
                headers={"X-Gotify-Key": "test_token"})
            mock_post.reset_mock()

            # Test sending a dictionary of messages
            self.notifier.send_notification(
                {"Category": ["Test message 1", "Test message 2"]})
            mock_post.assert_called_once_with(
                "http://example.com/message",
                json={"title": "Script Status",
                      "message": "\nCategory\nTest message 1\nTest message 2",
                      "priority": 5},
                headers={"X-Gotify-Key": "test_token"})
            mock_post.reset_mock()

            # Test sending a dictionary of messages with script name
            self.notifier.send_notification(
                {"Category": ["Test message 1", "Test message 2"]},
                script_name="Test Script")
            mock_post.assert_called_once_with(
                "http://example.com/message",
                json={"title": "Script Status",
                      "message": "\nTest Script - Category\nTest message 1\nTest message 2",
                      "priority": 5},
                headers={"X-Gotify-Key": "test_token"})
            mock_post.reset_mock()

            # Test sending a message with custom title and priority
            self.notifier.send_notification(
                "Test message", title="Custom Title", priority=10)
            mock_post.assert_called_once_with(
                "http://example.com/message",
                json={"title": "Custom Title", "message": "Test message",
                      "priority": 10},
                headers={"X-Gotify-Key": "test_token"})
            mock_post.reset_mock()

            # Test sending an empty message
            self.notifier.send_notification("")
            mock_post.assert_not_called()


    def test_format_dict_message(self):
        # Test formatting a dictionary of messages
        msg_dict = {"Category": ["Test message 1", "Test message 2"]}
        formatted_message = self.notifier._format_dict_message(msg_dict)
        self.assertEqual(
            formatted_message,
            "\nCategory\nTest message 1\nTest message 2")

        # Test formatting a dictionary of messages with script name
        formatted_message_with_script = self.notifier._format_dict_message(
            msg_dict, script_name="Test Script")
        self.assertEqual(
            formatted_message_with_script,
            "\nTest Script - Category\nTest message 1\nTest message 2")


    def test_parse_message_input(self):
        # Test parsing a single message
        parsed_message = self.notifier._parse_message_input("Test message")
        self.assertEqual(parsed_message, "Test message")

        # Test parsing a list of messages
        parsed_message_list = self.notifier._parse_message_input(
            ["Test message 1", "Test message 2"])
        self.assertEqual(
            parsed_message_list, "Test message 1\nTest message 2")

        # Test parsing a dictionary of messages
        parsed_message_dict = self.notifier._parse_message_input(
            {"Category": ["Test message 1", "Test message 2"]})
        self.assertEqual(
            parsed_message_dict, "\nCategory\nTest message 1\nTest message 2")

        # Test parsing a dictionary of messages with script name
        parsed_message_dict_with_script = self.notifier._parse_message_input(
            {"Category": ["Test message 1", "Test message 2"]},
            script_name="Test Script")
        self.assertEqual(
            parsed_message_dict_with_script,
            "\nTest Script - Category\nTest message 1\nTest message 2")

        # Test parsing an invalid message format
        parsed_invalid_message = self.notifier._parse_message_input(123)
        self.assertEqual(parsed_invalid_message, "")

    def test_ensure_strings(self):
        # Test ensuring all items in the list are strings
        items = [1, 2.5, "string", None, True]
        ensured_strings = self.notifier._ensure_strings(items)
        self.assertEqual(
            ensured_strings, ["1", "2.5", "string", "None", "True"])


if __name__ == '__main__':
    unittest.main()