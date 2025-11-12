#!/usr/bin/env python3
"""
Container Grouper - Group YANG paths by logical containers
Organizes paths into actionable containers (e.g., /interfaces/interface/config)
"""


class ContainerGrouper:
    """Group YANG paths into logical containers for action generation"""

    def __init__(self, yang_schema, list_registry=None):
        """
        Initialize with parsed YANG schema

        Args:
            yang_schema: Dict from datastore
                {
                    'module_name': {
                        'paths': {path: metadata, ...},
                        'path_count': N
                    }
                }
        """
        self.yang_schema = yang_schema
        self.list_registry = list_registry or {}

    def group_by_container(self, min_params=1):
        """
        Group paths by their parent container and track container type

        Args:
            min_params: Minimum parameters to create action (default: 1)

        Returns:
            dict: Grouped containers with type metadata
                {
                    'module_name': {
                        '/container/path': {
                            'paths': {leaf_path: metadata, ...},
                            'param_count': N,
                            'is_writable': True/False,
                            'container_type': 'config' or 'state',
                            'supported_operations': ['get', ...] or ['get']
                        }
                    }
                }
        """
        grouped = {}

        for module_name, module_data in self.yang_schema.items():
            paths = module_data.get("paths", {})
            if not paths:
                continue

            module_containers = {}

            for path, metadata in paths.items():
                # Find parent container
                container_path = self._get_container_path(path)
                if not container_path:
                    continue

                # Initialize container group with type detection
                if container_path not in module_containers:
                    # Detect container type from first path
                    is_writable = metadata.get("config", True) and not metadata.get(
                        "readonly", False
                    )
                    container_type = self._detect_container_type(
                        container_path, is_writable
                    )

                    # Check if this container is under a list
                    list_info = self._get_list_info(module_name, container_path)

                    module_containers[container_path] = {
                        "paths": {},
                        "param_count": 0,
                        "is_writable": is_writable,
                        "container_type": container_type,
                        "supported_operations": self._get_supported_operations(
                            is_writable
                        ),
                        "list_info": list_info,
                    }

                # Add path to container
                if not metadata.get("is_list_key", False):
                    module_containers[container_path]["paths"][path] = metadata
                    module_containers[container_path]["param_count"] += 1

            # Filter out containers with too few parameters
            filtered_containers = {
                path: data
                for path, data in module_containers.items()
                if data["param_count"] >= min_params
                or data.get("list_info", {}).get("is_list", False)
            }

            if filtered_containers:
                grouped[module_name] = filtered_containers

        return grouped

    def _get_container_path(self, full_path):
        """
        Extract container path from full leaf path

        Args:
            full_path: Full YANG path (e.g., '/interfaces/interface/config/mtu')

        Returns:
            str: Container path (e.g., '/interfaces/interface/config')
                 or None if not suitable for grouping

        Examples:
            '/interfaces/interface/config/mtu' -> '/interfaces/interface/config'
            '/interfaces/interface/state/oper-status' -> '/interfaces/interface/state'
            '/system/config/hostname' -> '/system/config'
        """
        # Remove leading slash and split
        parts = full_path.strip("/").split("/")

        # Need at least 2 parts (container + leaf)
        if len(parts) < 2:
            return None

        # Container is everything except the last part (the leaf)
        container_parts = parts[:-1]

        # Prefer containers ending with 'config' or 'state'
        # This follows OpenConfig/IETF conventions
        container_path = "/" + "/".join(container_parts)

        return container_path

    def get_container_summary(self, grouped_containers):
        """
        Get summary statistics of grouped containers including type breakdown

        Args:
            grouped_containers: Output from group_by_container()

        Returns:
            dict: Summary statistics with container type breakdown
        """
        total_modules = len(grouped_containers)
        total_containers = sum(
            len(containers) for containers in grouped_containers.values()
        )
        total_paths = sum(
            data["param_count"]
            for module_data in grouped_containers.values()
            for data in module_data.values()
        )

        # Count by container type
        config_containers = sum(
            1
            for module_data in grouped_containers.values()
            for data in module_data.values()
            if data.get("is_writable", True)
        )
        state_containers = total_containers - config_containers
        return {
            "total_modules": total_modules,
            "total_containers": total_containers,
            "total_paths": total_paths,
            "config_containers": config_containers,
            "state_containers": state_containers,
            "avg_paths_per_container": (
                total_paths / total_containers if total_containers > 0 else 0
            ),
        }

    def _detect_container_type(self, container_path, is_writable):
        """
        Detect container type based on path convention and writability

        OpenConfig/IETF Convention:
            Paths ending in '/config' are writable configuration
            Paths ending in '/state' are read-only state data

        Args:
            container_path: Container path (e.g., '/interfaces/interface/config')
            is_writable: Whether container has writable leaves

        Returns:
            str: 'config' or 'state'

        Examples:
            '/interfaces/interface/config' -> 'config'
            '/interfaces/interface/state' -> 'state'
            '/system/logging/console' (writable) -> 'config'
            '/system/logging/console' (readonly) -> 'state'
        """
        # Check path-based convention first (most reliable)
        if container_path.endswith("/config"):
            return "config"
        elif container_path.endswith("/state"):
            return "state"

        # Fall back to writability check
        return "config" if is_writable else "state"

    def _get_supported_operations(self, is_writable):
        """
        Determine which gNMI operations are supported for this container

        Args:
            is_writable: Whether the container is writable

        Returns:
            list: Supported gNMI operations
                Writable: ['get', 'update', 'replace', 'delete']
                Read-only: ['get']

        Examples:
            is_writable=True  -> ['get', 'update', 'replace', 'delete']
            is_writable=False -> ['get']
        """
        if is_writable:
            return ["get", "update", "replace", "delete"]
        else:
            return ["get"]  # Read-only: only get supported

    def _get_list_info(self, module_name, container_path):
        """
        Get list metadata for a container path, including ALL ancestor lists

        For nested lists like /network-instances/network-instance/vlans/vlan/config,
        we need keys for BOTH parent lists:
        - /network-instances/network-instance → key: name
        - /network-instances/network-instance/vlans/vlan → key: vlan-id
        """
        module_lists = self.list_registry.get(module_name, {})

        # Find ALL matching list paths (for nested lists)
        matching_lists = []

        for list_path, list_meta in module_lists.items():
            if container_path.startswith(list_path + "/"):
                matching_lists.append(
                    {
                        "list_path": list_path,
                        "keys": list_meta["keys"],
                        "length": len(list_path),
                    }
                )

        if not matching_lists:
            return {"is_list": False}

        # Sort by length (shortest to longest) to build path correctly
        matching_lists.sort(key=lambda x: x["length"])

        # Combine all keys but preserve which list they belong to
        all_keys = []
        for match in matching_lists:
            for key in match["keys"]:
                # Add list_path to each key so we know where to insert it
                key_with_path = key.copy()
                key_with_path["list_path"] = match["list_path"]
                all_keys.append(key_with_path)

        # Use the longest (innermost) list path as the primary list path
        innermost_list = matching_lists[-1]

        return {
            "is_list": True,
            "list_path": innermost_list["list_path"],
            "keys": all_keys,
            "all_list_paths": [m["list_path"] for m in matching_lists],
        }
