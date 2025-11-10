#!/usr/bin/env python3

"""
Type Extractor - Extract type metadata from YANG nodes
Handles enum extraction from imported groupings (fixes the enum bug!)
"""


class TypeExtractor:
    """Extract type information from YANG type statements"""

    def extract_type_info(self, type_stmt):
        """
        Extract complete type information including enums from imported modules

        Args:
            type_stmt: Pyang type statement

        Returns:
            dict: Type information including enums, ranges, patterns, etc.
        """
        type_info = {}

        # Resolve typedef references (handles imported modules)
        # CRITICAL for cross-module enum extraction!
        resolved_type_stmt = type_stmt
        type_name = type_stmt.arg

        if hasattr(type_stmt, "i_typedef") and type_stmt.i_typedef:
            typedef = type_stmt.i_typedef
            typedef_type_stmt = typedef.search_one("type")
            if typedef_type_stmt:
                resolved_type_stmt = typedef_type_stmt
                type_name = typedef_type_stmt.arg

        # Store base type
        type_info["type"] = type_name

        # Extract type-specific information from resolved type statement

        match type_name:
            case "enumeration":
                enum_stmts = resolved_type_stmt.search("enum")
                if enum_stmts:
                    type_info["enum"] = [e.arg for e in enum_stmts]

            case "union":
                union_types = resolved_type_stmt.search("type")
                if union_types:
                    type_info["union_types"] = [t.arg for t in union_types]

            case [
                "int8",
                "int16",
                "int32",
                "int64",
                "uint8",
                "uint16",
                "uint32",
                "uint64",
            ]:
                range_stmt = resolved_type_stmt.search_one("range")
                if range_stmt:
                    type_info["range"] = self._parse_range(range_stmt.arg)

            case "string":
                length_stmt = resolved_type_stmt.search_one("length")
                if length_stmt:
                    type_info["length"] = self._parse_range(length_stmt.arg)

                pattern_stmts = resolved_type_stmt.search("pattern")
                if pattern_stmts:
                    type_info["patterns"] = [p.arg for p in pattern_stmts]

            case "leafref":
                path_stmt = resolved_type_stmt.search_one("path")
                if path_stmt:
                    type_info["leafref_path"] = path_stmt.arg

            case "identityref":
                base_stmt = resolved_type_stmt.search_one("base")
                if base_stmt:
                    type_info["identity_base"] = base_stmt.arg

            case "decimal64":
                fraction_stmt = resolved_type_stmt.search_one("fraction-digits")
                if fraction_stmt:
                    type_info["fraction_digits"] = int(fraction_stmt.arg)

            case "bits":
                bit_stmts = resolved_type_stmt.search("bit")
                if bit_stmts:
                    type_info["bits"] = [b.arg for b in bit_stmts]

        return type_info

    def _parse_range(self, range_str):
        """
        Parse YANG range/length statement (simplified)

        Extracts numeric min/max bounds and preserves raw string.
        Pipe (|) in YANG means OR - value must match one of the ranges.

        Args:
            range_str: YANG range string (e.g., "1..10 | 100..1000")

        Returns:
            dict: {'raw': str, 'min': int, 'max': int}

        Examples:
            "1..100" -> {'raw': '1..100', 'min': 1, 'max': 100}
            "1..10 | 100..1000" -> {'raw': '1..10 | 100..1000', 'min': 1, 'max': 1000}
        """
        result = {"raw": range_str}

        try:
            # Extract all numbers: replace pipes with ".." then split
            # "1..10 | 100..1000" -> "1..10 .. 100..1000" -> ["1", "10", "100", "1000"]
            parts = range_str.replace("|", "..").split("..")
            numbers = []

            for part in parts:
                try:
                    numbers.append(int(part.strip()))
                except ValueError:
                    pass  # Skip non-numeric (like YANG 'min'/'max' keywords)

            # Store overall min/max if we found any numbers
            if numbers:
                result["min"] = min(numbers)
                result["max"] = max(numbers)

        except Exception:
            pass  # Just return raw if parsing fails

        return result
