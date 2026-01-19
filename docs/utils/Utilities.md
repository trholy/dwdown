# Utils module
## Utilities

### Overview

The `Utilities` class provides general utility methods, such as converting objects to lists and variable mapping.

### Constructor

```python
Utilities()
```

Initializes the Utilities.

### Methods

#### `_string_to_list`

```python
_string_to_list(obj, flatten: bool = False) -> list
```

Converts an object to a list. If the object is a string, it returns a list containing that string.

#### Parameters

- `obj` : `Any`
  - The object to convert to a list.
- `flatten` : `bool`, default=`False`
  - If True, flattens the list if `obj` is a list.

#### Returns

- `list`
  - A list representation of the object.

#### `_flatten_list`

```python
_flatten_list(obj: list | str) -> list
```

Flattens a list of lists or a string into a single list.

#### Parameters

- `obj` : `list | str`
  - List or string to flatten.

#### Returns

- `list`
  - Flattened list.

#### `_variable_mapping`

```python
@staticmethod
_variable_mapping(variables: list[str], mapping_dictionary: dict[str, str]) -> list[str]
```

Maps variables to their corresponding values in the mapping dictionary.

#### Parameters

- `variables` : `list[str]`
  - List of variable names to map.
- `mapping_dictionary` : `dict[str, str]`
  - Dictionary mapping variable names to their values.

#### Returns

- `list[str]`
  - List of mapped variable names.

#### `_extract_additional_pattern`

```python
@staticmethod
_extract_additional_pattern(filename: str) -> int | None
```

Extracts a numeric pattern from a filename that matches the pattern "_{digits}_" before the variable name.

#### Parameters

- `filename` : `str`
  - The filename to extract the pattern from.

#### Returns

- `int | None`
  - The extracted numeric pattern as an integer, or None if no pattern is found.
