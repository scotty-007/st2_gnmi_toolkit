#!/usr/bin/env python3
"""
AST Walker - Traverse YANG AST and extract all paths (config and state)
Walks pyang's i_children tree which automatically expands:
- Groupings (including imported ones)
- Augments
- Uses statements
"""
from type_extractor import TypeExtractor


class ASTWalker:
    """Walk YANG AST and extract all leaf paths (config=true and config=false)"""

    def __init__(self):
        self.type_extractor = TypeExtractor()

    def extract_paths(self, pyang_module):
        """
        Extract all paths from a YANG module (config and state)

        Args:
            pyang_module: Pyang module object

        Returns:
            dict: {path: {type, config, description, enum, etc.}}
        """
        paths = {}

        # Walk all top-level children
        # Note: i_children already has groupings expanded!
        for child in pyang_module.i_children:
            self._walk_node(child, [], paths)

        return paths

    def _walk_node(self, node, path_stack, paths, depth=0):
        """
        Recursively walk YANG AST and extract all paths

        Args:
            node: Current pyang statement node
            path_stack: Current path components (list)
            paths: Dict to populate with paths
            depth: Recursion depth (safety limit)
        """
        # Safety: prevent infinite recursion
        if depth > 50:
            return

        # Build current path
        current_path = path_stack + [node.arg]
        full_path = "/" + "/".join(current_path)

        # Handle different node types
        if node.keyword == "leaf":
            # Leaf node - extract ALL leaves (config and state)
            leaf_info = self._extract_leaf_info(node)
            if leaf_info:
                paths[full_path] = leaf_info

        elif node.keyword == "leaf-list":
            # Leaf-list - similar to leaf but multiple values
            leaf_info = self._extract_leaf_info(node)
            if leaf_info:
                leaf_info["is_list"] = True
                paths[full_path] = leaf_info

        elif node.keyword in ["container", "list"]:
            # Container/List - recurse into children
            # Note: pyang's i_children already expands groupings and augments!
            if hasattr(node, "i_children") and node.i_children:
                for child in node.i_children:
                    self._walk_node(child, current_path, paths, depth + 1)

        elif node.keyword == "choice":
            # Choice statement - walk cases
            if hasattr(node, "i_children"):
                for child in node.i_children:
                    if child.keyword == "case":
                        # Don't add 'case' to path, just recurse into case children
                        if hasattr(child, "i_children"):
                            for case_child in child.i_children:
                                self._walk_node(
                                    case_child, current_path, paths, depth + 1
                                )

    def _get_config_status(self, node):
        """
        Get the config status of a node (config true/false)

        In YANG, config is inherited from parent if not explicitly set.
        Returns True if config=true (writable), False if config=false (read-only state)
        """
        # Check node's config statement
        config_stmt = node.search_one("config")
        if config_stmt:
            return config_stmt.arg == "true"

        # Check parent's config (inherited)
        parent = node.parent
        while parent:
            config_stmt = parent.search_one("config")
            if config_stmt:
                return config_stmt.arg == "true"
            parent = parent.parent

        # Default in YANG is config=true
        return True

    def _extract_leaf_info(self, leaf_node):
        """
        Extract complete metadata from a leaf node

        Args:
            leaf_node: Pyang leaf statement

        Returns:
            dict: Leaf metadata (type, description, mandatory, default, etc.)
        """
        # Get actual config status from node
        is_config = self._get_config_status(leaf_node)

        info = {"config": is_config, "readonly": not is_config}

        # Get description
        desc_stmt = leaf_node.search_one("description")
        if desc_stmt:
            info["description"] = desc_stmt.arg.strip()

        # Get mandatory
        mandatory_stmt = leaf_node.search_one("mandatory")
        if mandatory_stmt:
            info["mandatory"] = mandatory_stmt.arg == "true"

        # Get default value
        default_stmt = leaf_node.search_one("default")
        if default_stmt:
            info["default"] = default_stmt.arg

        # Get units
        units_stmt = leaf_node.search_one("units")
        if units_stmt:
            info["units"] = units_stmt.arg

        # Get type information (THE CRITICAL PART for enum extraction!)
        type_stmt = leaf_node.search_one("type")
        if type_stmt:
            type_info = self.type_extractor.extract_type_info(type_stmt)
            info.update(type_info)

        return info
