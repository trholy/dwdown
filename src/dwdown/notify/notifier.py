import logging
from typing import Optional, Union

import requests

# Configure logging to remove the default prefix
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
)


class Notifier:
    """
    A notification sender for Gotify servers.

    This class sends messages to a Gotify server using HTTP(S) requests.
    """

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
        :param priority: Message priority level (default: 5).
        :param secure: Whether to use HTTPS (default: False).
        """
        self.server_url = f"https://{server_url}"\
            if secure else f"http://{server_url}"
        self.token = token
        self.priority = priority
        self.logger = logging.getLogger(__name__)

    def send_notification(
            self,
            message: Union[list[str], str, dict[str, list[str]]],
            script_name: Optional[str] = None,
            title: Optional[str] = None,
            priority: Optional[int] = None,
    ) -> None:
        """
        Sends a notification to the Gotify server.

        :param message: A single message, a list of messages,
         or a dictionary with categorized messages.
        :param script_name: The name of the script.
        :param title: The title of the notification (optional).
        :param priority: Message priority level (default: None).
        :return: None
        """
        headers = {"X-Gotify-Key": self.token}
        title = title or "Script Status"

        full_message = self.parse_message_input(script_name, message)

        if not full_message.strip():
            self.logger.warning(
                "No valid message content to send. Skipping notification."
            )
            return

        payload = {
            "title": title,
            "message": full_message,
            "priority": priority or self.priority
        }

        try:
            response = requests.post(
                self.server_url, json=payload, headers=headers
            )
            response.raise_for_status()
            self.logger.info("Notification sent successfully!")
        except requests.RequestException as e:
            self.logger.error(f"Failed to send notification: {e}")

    @staticmethod
    def format_dict_message(
            script_name: Optional[str],
            msg_dict: dict[str, list[str]]
    ) -> str:
        """
        Formats a dictionary of messages into a structured string.

        :param script_name: The name of the script (optional).
        :param msg_dict: A dictionary where keys are categories and values
         are lists of messages.
        :return: A formatted string representation of the dictionary.
        """
        message_list = []
        for category, values in msg_dict.items():
            category_header = f"\n{script_name} - {category}"\
                if script_name else f"\n{category}"
            message_list.append(category_header)
            if not all(isinstance(item, str) for item in values):
                values = [str(item) for item in values]
            message_list.extend(values)
        return "\n".join(message_list)

    def parse_message_input(
            self,
            script_name: Optional[str],
            msg_input: Union[list[str], str, dict[str, list[str]]]
    ) -> str:
        """
        Parses input message, ensuring it's properly formatted for notification.

        :param script_name: The name of the script (optional).
        :param msg_input: The message input, which can be a string,
         a list of strings, or a dictionary.
        :return: A formatted string ready to be sent as a notification.
        """
        if isinstance(msg_input, str):
            msg_input = [msg_input]
        if isinstance(msg_input, dict):
            return self.format_dict_message(script_name, msg_input)
        elif isinstance(msg_input, list):
            msg_list = msg_input.copy()  # Avoid modifying the original list
            if script_name:
                msg_list.insert(0, f"{script_name}")
            return "\n".join(msg_list)
        else:
            self.logger.error(
                "Invalid message format. Expected str, list, or dict."
            )
            return ""
