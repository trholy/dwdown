import re


class Utilities:
    def __init__(self):
        """
        Initializes the Utilities.

        """
    def _string_to_list(
            self,
            obj,
            flatten: bool = False
    ) -> list:
        """
        Converts an object to a list. If the object is a string,
         it returns a list containing that string.
        If the object is already a list, it returns the list. If `flatten` is
         True and the object is a list, it flattens the list.

        :param obj: The object to convert to a list.
        :param flatten: If True, flattens the list if `obj` is a list
         (default is False).
        :return: A list representation of the object.
        """
        if isinstance(obj, str):
            return [obj]
        elif isinstance(obj, list):
            if flatten:
                return self._flatten_list(obj)
            return obj
        else:
            return []

    def _flatten_list(self, obj: list | str) -> list:
        """
        Flattens a list of lists or a string into a single list.

        :param obj: List or string to flatten.
        :return: Flattened list.
        """
        if isinstance(obj, str):
            return [obj]
        elif isinstance(obj, list):
            flattened = []
            for item in obj:
                flattened.extend(self._flatten_list(item))
            return flattened
        else:
            raise TypeError(f"Expected a list or string, but got: {type(obj)}")

    @staticmethod
    def _variable_mapping(
            variables: list[str],
            mapping_dictionary: dict[str, str]
    ) -> list[str]:
        """
        Maps variables to their corresponding values in the mapping dictionary.
         If a variable is not found in the dictionary, it remains unchanged.

        :param variables: List of variable names to map.
        :param mapping_dictionary: Dictionary mapping variable names to their values.
        :return: List of mapped variable names.
        """
        if isinstance(variables, list):
            return [mapping_dictionary.get(var, var) for var in variables]
        return []

    @staticmethod
    def _extract_additional_pattern(filename: str) -> int | None:
        """
        Extracts a numeric pattern from a filename that matches the pattern
         "_{digits}_" before the variable name.

        :param filename: The filename to extract the pattern from.
        :return: The extracted numeric pattern as an integer, or None
         if no pattern is found.
        """
        # Matches "_{digits}_" before variable name
        match = re.search(r"_(\d+)_([a-zA-Z_]+)\.csv$", filename)
        if match:
            pattern = match.group(1)  # Extract the numeric pattern
            return int(pattern)
        return None
