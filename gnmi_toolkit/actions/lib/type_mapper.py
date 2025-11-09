#!/usr/bin/env python3
"""
Type Mapper - Map YANG types to StackStorm parameter specifications
Converts YANG type metadata (enum, range, pattern) to ST2 action parameters
"""


class TypeMapper:
    """Map YANG types to StackStorm parameter types with constraints"""

    # MongoDB 8-byte signed integer limits (ST2 uses MongoDB for storage)
    # Max value: 2^63 - 1 = 9223372036854775807
    # Min value: -(2^63) = -9223372036854775808
    MONGODB_INT_MAX = 9223372036854775807
    MONGODB_INT_MIN = -9223372036854775808

    # YANG to ST2 base type mapping
    YANG_TO_ST2_TYPE_MAP = {
        # Integer types
        "int8": "integer",
        "int16": "integer",
        "int32": "integer",
        "int64": "integer",
        "uint8": "integer",
        "uint16": "integer",
        "uint32": "integer",
        "uint64": "integer",
        # String types
        "string": "string",
        "binary": "string",
        # Boolean
        "boolean": "boolean",
        # Decimal
        "decimal64": "number",
        # Enumeration
        "enumeration": "string",  # with enum constraint
        # Special types
        "union": "string",  # Use first type or string
        "leafref": "string",  # Path reference
        "identityref": "string",  # Identity reference
        "empty": "boolean",  # Empty type maps to boolean
        "bits": "array",  # Bit flags
    }

    def map_yang_to_st2_parameter(self, path_metadata):
        """
        Map YANG path metadata to StackStorm parameter specification

        Args:
            path_metadata: Dict with YANG metadata from parser
                {
                    'type': 'uint16',
                    'range': {'min': 68, 'max': 65535},
                    'description': '...',
                    'mandatory': False,
                    'default': '1500'
                }

        Returns:
            dict: ST2 parameter specification
                {
                    'type': 'integer',
                    'required': False,
                    'description': '...',
                    'minimum': 68,
                    'maximum': 65535,
                    'default': 1500
                }
        """
        yang_type = path_metadata.get("type", "string")

        # Start with base type mapping
        st2_type = self.YANG_TO_ST2_TYPE_MAP.get(yang_type, "string")

        param_spec = {
            "type": st2_type,
            "required": False,  # Most YANG leaves are optional
        }

        # Add description if available
        if "description" in path_metadata:
            desc = path_metadata["description"].replace('"', '\\"').replace("\n", " ")
            # Truncate very long descriptions
            if len(desc) > 200:
                desc = desc[:197] + "..."
            param_spec["description"] = desc

        # Add mandatory flag
        if path_metadata.get("mandatory", False):
            param_spec["required"] = True

        # Add default value (convert to appropriate type)
        if "default" in path_metadata:
            default_val = path_metadata["default"]
            param_spec["default"] = self._convert_default_value(default_val, st2_type)

        # Apply type-specific constraints

        # Enumeration constraints
        if "enum" in path_metadata and path_metadata["enum"]:
            param_spec["enum"] = path_metadata["enum"]

        # Integer range constraints (cap at MongoDB limits)
        if "range" in path_metadata:
            range_info = path_metadata["range"]
            if "min" in range_info:
                min_val = range_info["min"]
                # Cap at MongoDB minimum
                param_spec["minimum"] = max(min_val, self.MONGODB_INT_MIN)
            if "max" in range_info:
                max_val = range_info["max"]
                # Cap at MongoDB maximum
                param_spec["maximum"] = min(max_val, self.MONGODB_INT_MAX)

        # String length constraints
        if "length" in path_metadata:
            length_info = path_metadata["length"]
            # ST2 doesn't have native minLength/maxLength, store in description
            if "min" in length_info or "max" in length_info:
                length_desc = f" (length: {length_info.get('min', 0)}-{length_info.get('max', 'unlimited')})"
                param_spec["description"] = (
                    param_spec.get("description", "") + length_desc
                )

        # String pattern constraints
        if "patterns" in path_metadata and path_metadata["patterns"]:
            # Use first pattern if multiple
            param_spec["pattern"] = path_metadata["patterns"][0]

        # Union types - use string with description
        if "union_types" in path_metadata:
            union_types = ", ".join(path_metadata["union_types"])
            param_spec["description"] = (
                param_spec.get("description", "") + f" (union: {union_types})"
            )

        # Leafref - add path reference in description
        if "leafref_path" in path_metadata:
            param_spec["description"] = (
                param_spec.get("description", "")
                + f" (ref: {path_metadata['leafref_path']})"
            )

        # Identity reference
        if "identity_base" in path_metadata:
            param_spec["description"] = (
                param_spec.get("description", "")
                + f" (identity: {path_metadata['identity_base']})"
            )

        return param_spec

    def _convert_default_value(self, default_str, st2_type):
        """
        Convert YANG default string to appropriate Python/ST2 type

        Args:
            default_str: Default value as string from YANG
            st2_type: Target ST2 type

        Returns:
            Converted value (int, bool, str, etc.)
        """
        try:
            if st2_type == "integer":
                return int(default_str)
            elif st2_type == "number":
                return float(default_str)
            elif st2_type == "boolean":
                return default_str.lower() in ("true", "1", "yes")
            else:
                return default_str
        except (ValueError, AttributeError):
            # If conversion fails, return as string
            return default_str
