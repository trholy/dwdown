import logging
from urllib.parse import urlparse, urlunparse

import requests

from ..utils import Utilities


class Notifier(Utilities):
    def __init__(
            self,
            server_url: str,
            token: str,
            priority: int = 5,
            secure: bool = False
    ):
        """
        Initializes the Notifier with server details and authentication token.

        :param server_url: The URL of the Gotify server.
        :param token: Authentication token for the server.
        :param priority: Default message priority level (default: 5).
        :param secure: Use HTTPS if True, otherwise HTTP (default: False).
        """
        # Normalize URL
        parsed = urlparse(server_url)

        # If no scheme, treat entire input as netloc
        if not parsed.scheme:
            netloc = parsed.path
        else:
            netloc = parsed.netloc or parsed.path

        # Override scheme based on secure flag
        scheme = "https" if secure else "http"

        # Rebuild full URL
        self.server_url = urlunparse((scheme, netloc, '/message', '', '', ''))
        self.token = token
        self.priority = priority

        Utilities.__init__(self)

        self._logger = logging.getLogger(__name__)

    def send_notification(
            self,
            message: list[str] | str | dict[str, list[str]],
            script_name: str | None = None,
            title: str | None = None,
            priority: int | None = None,
    ) -> None:
        """
        Sends a notification to the Gotify server.

        :param message: Single message, list of messages,
         or categorized dictionary.
        :param script_name: The name of the script.
        :param title: The title of the notification (optional).
        :param priority: Message priority (optional,
        default: class default priority).
        :return: None
        """
        full_message = self._parse_message_input(message, script_name)

        if not full_message.strip():
            self._logger.warning(
                "No valid message content to send. Skipping notification."
            )
            return

        payload = {
            "title": title or "Script Status",
            "message": full_message,
            "priority": priority or self.priority
        }
        headers = {"X-Gotify-Key": self.token}

        try:
            response = requests.post(
                self.server_url, json=payload, headers=headers
            )
            response.raise_for_status()

            self._logger.info("Notification sent successfully!")
        except requests.RequestException as e:
            self._logger.error(f"Failed to send notification: {e}")

    def _format_dict_message(
            self,
            msg_dict: dict[str, list[str]],
            script_name: str | None = None
    ) -> str:
        """
        Formats a dictionary of messages into a structured string.

        :param script_name: The name of the script (optional).
        :param msg_dict: Dictionary where keys are categories and values
         are lists of messages.
        :return: Formatted string representation of the dictionary.
        """
        message_parts = []

        for category, values in msg_dict.items():
            category_header = f"\n{script_name} - {category}" \
                if script_name else f"\n{category}"
            values = self._ensure_strings(values)
            message_parts.append(category_header)
            message_parts.extend(values)

        return "\n".join(message_parts)

    def _parse_message_input(
            self,
            msg_input: list[str] | str | dict[str, list[str]],
            script_name: str | None = None
    ) -> str:
        """
        Parses the input message and ensures proper formatting
         for notifications.

        :param script_name: The name of the script.
        :param msg_input: Message input (string, list, or dictionary).
        :return: Formatted string ready to send as a notification.
        """
        if isinstance(msg_input, str):
             msg_input = [msg_input]  # Convert single message to list

        if isinstance(msg_input, dict):
            return self._format_dict_message(msg_input, script_name)

        if isinstance(msg_input, list):
            msg_list = msg_input.copy()
            msg_list = self._ensure_strings(msg_list)
            if script_name:
                msg_list.insert(0, script_name)
            return "\n".join(msg_list)
        self._logger.error(
            "Invalid message format. Expected str, list, or dict."
        )
        return ""

    @staticmethod
    def _ensure_strings(items: list) -> list[str]:
        """
        Ensures all items in the list are strings.

        :param items: List of items to convert to strings.
        :return: List of strings.
        """
        return [str(item) for item in items]
