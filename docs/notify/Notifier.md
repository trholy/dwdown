# Notify module
## Notifier

### Overview

The `Notifier` class is designed to send notifications to a Gotify server. It supports sending simple string messages, lists of messages, or categorized dictionaries of messages, with support for priority levels and secure connections.

### Constructor

```python
Notifier(
    server_url: str,
    token: str,
    priority: int = 5,
    secure: bool = False
)
```

#### Parameters

- `server_url` : `str`
  - The URL of the Gotify server.
- `token` : `str`
  - Authentication token for the server.
- `priority` : `int`, default=`5`
  - Default message priority level.
- `secure` : `bool`, default=`False`
  - Use HTTPS if True, otherwise HTTP.

### Methods

#### `send_notification`

```python
send_notification(
    message: list[str] | str | dict[str, list[str]],
    script_name: str | None = None,
    title: str | None = None,
    priority: int | None = None
) -> None
```

Sends a notification to the Gotify server.

#### Parameters

- `message` : `list[str] | str | dict[str, list[str]]`
  - Single message, list of messages, or categorized dictionary.
- `script_name` : `str | None`, default=`None`
  - The name of the script.
- `title` : `str | None`, default=`None`
  - The title of the notification. Defaults to "Script Status".
- `priority` : `int | None`, default=`None`
  - Message priority. Defaults to the class instance's priority.

#### `_format_dict_message`

```python
_format_dict_message(msg_dict: dict[str, list[str]], script_name: str | None = None) -> str
```

Formats a dictionary of messages into a structured string.

#### Parameters

- `msg_dict` : `dict[str, list[str]]`
  - Dictionary where keys are categories and values are lists of messages.
- `script_name` : `str | None`, default=`None`
  - The name of the script.

#### Returns

- `str`
  - Formatted string representation of the dictionary.

#### `_parse_message_input`

```python
_parse_message_input(
    msg_input: list[str] | str | dict[str, list[str]],
    script_name: str | None = None
) -> str
```

Parses the input message and ensures proper formatting for notifications.

#### Parameters

- `msg_input` : `list[str] | str | dict[str, list[str]]`
  - Message input (string, list, or dictionary).
- `script_name` : `str | None`, default=`None`
  - The name of the script.

#### Returns

- `str`
  - Formatted string ready to send as a notification.

#### `_ensure_strings`

```python
@staticmethod
_ensure_strings(items: list) -> list[str]
```

Ensures all items in the list are strings.

#### Parameters

- `items` : `list`
  - List of items to convert to strings.

#### Returns

- `list[str]`
  - List of strings.
