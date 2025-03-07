# Notify module
## Notifier

### Overview

The `Notifier` class is designed to send messages to a Gotify server using HTTP(S) requests. It provides methods to initialize the notifier with server details and authentication token, and to send notifications with customizable priority and content.

### Constructor

```python
Notifier(server_url: str, token: str, priority: int = 5, secure: bool = False)
```

#### Parameters

- `server_url` : `str`
  - The URL of the Gotify server.
- `token` : `str`
  - Authentication token for the server.
- `priority` : `int`, default=5
  - Message priority level.
- `secure` : `bool`, default=False
  - Whether to use HTTPS.

### Methods

#### send_notification

```python
send_notification(message: Union[list[str], str, dict[str, list[str]]], script_name: Optional[str] = None, title: Optional[str] = None, priority: Optional[int] = None) -> None
```

Sends a notification to the Gotify server.

##### Parameters

- `message` : `Union[list[str], str, dict[str, list[str]]]`
  - A single message, a list of messages, or a dictionary with categorized messages.
- `script_name` : `Optional[str]`, default=None
  - The name of the script.
- `title` : `Optional[str]`, default=None
  - The title of the notification.
- `priority` : `int`, default=None
  - Message priority level.

##### Returns

- `None`

#### format_dict_message

```python
@staticmethod
format_dict_message(script_name: Optional[str], msg_dict: dict[str, list[str]]) -> str
```

Formats a dictionary of messages into a structured string.

##### Parameters

- `script_name` : `Optional[str]`, default=None
  - The name of the script.
- `msg_dict` : `dict[str, list[str]]`
  - A dictionary where keys are categories and values are lists of messages.

##### Returns

- `str`
  - A formatted string representation of the dictionary.

#### parse_message_input

```python
parse_message_input(script_name: Optional[str], msg_input: Union[list[str], str, dict[str, list[str]]]) -> str
```

Parses input message, ensuring it's properly formatted for notification.

##### Parameters

- `script_name` : `Optional[str]`, default=None
  - The name of the script.
- `msg_input` : `Union[list[str], str, dict[str, list[str]]]`
  - The message input, which can be a string, a list of strings, or a dictionary.

##### Returns

- `str`
  - A formatted string ready to be sent as a notification.

---
