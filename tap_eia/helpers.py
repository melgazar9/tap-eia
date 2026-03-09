"""Helper functions for tap-eia."""

import re
import uuid


def clean_strings(string: str) -> str:
    """Clean strings to snake_case format."""
    if not string:
        return string

    # Replace camelCase and PascalCase with snake_case
    string = re.sub(r"(?<!^)(?=[A-Z])", "_", string).lower()

    # Replace any non-alphanumeric characters with underscores
    string = re.sub(r"[^a-zA-Z0-9_]", "_", string)

    # Remove consecutive underscores
    string = re.sub(r"_+", "_", string)

    # Remove leading/trailing underscores
    string = string.strip("_")

    return string


def clean_json_keys(data):
    """Recursively clean JSON keys to snake_case."""
    if isinstance(data, dict):
        return {clean_strings(key): clean_json_keys(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [clean_json_keys(item) for item in data]
    else:
        return data


def generate_surrogate_key(data: dict, namespace=uuid.NAMESPACE_DNS) -> str:
    """Generate a surrogate key from record data."""
    key_values = [str(data.get(field, "")) for field in sorted(data.keys())]
    key_string = "|".join(key_values)
    return str(uuid.uuid5(namespace, key_string))
