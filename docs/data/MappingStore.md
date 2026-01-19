# Data module
## MappingStore

### Overview

The `MappingStore` class manages a predefined dictionary that maps internal variable names to their corresponding values, typically used for standardizing variable names across different data sources or formats.

### Constructor

```python
MappingStore()
```

Initializes the MappingStore with a predefined mapping dictionary.

### Methods

#### `_get_mapping_dict`

```python
@staticmethod
_get_mapping_dict() -> dict[str, str]
```

Returns a predefined mapping dictionary.

#### Returns

- `dict[str, str]`
  - A dictionary mapping variable names to their corresponding values.

#### `get_mapping_dict`

```python
get_mapping_dict() -> dict[str, str]
```

Returns the mapping dictionary.

#### Returns

- `dict[str, str]`
  - The dictionary mapping variable names to their corresponding values.
