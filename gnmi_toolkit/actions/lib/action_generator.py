#!/usr/bin/env python3
"""
Action Generator - Generate StackStorm actions from YANG containers
Renders Jinja2 templates and writes action files to device packs
"""
import os
import re
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, Template
from type_mapper import TypeMapper


class ActionGenerator:
    """Generate StackStorm actions from YANG container groups"""

    def __init__(self, template_dir, output_dir):
        """
        Initialize action generator with Jinja2 environment

        Args:
            template_dir: Path to Jinja2 templates directory
            output_dir: Path to write generated action files
        """
        self.template_dir = template_dir
        self.output_dir = output_dir
        self.type_mapper = TypeMapper()

        # Create output directory if needed
        os.makedirs(output_dir, exist_ok=True)

        # Setup Jinja2 environment
        self.jinja_env = Environment(
            loader=FileSystemLoader(template_dir), trim_blocks=True, lstrip_blocks=True
        )

    def generate_action_for_container(
        self, device_name, module_name, container_path, container_data, pack_name
    ):
        """
        Generate action files (.yaml and .py) for a YANG container

        Args:
            device_name: Device identifier for action naming
            module_name: YANG module name
            container_path: Container path (e.g., '/interfaces/interface/config')
            container_data: Dict with 'paths' and 'param_count'
            pack_name: Target StackStorm pack name

        Returns:
            dict: Generation result
                {
                    'success': True,
                    'action_name': 'device_192_168_1_50_openconfig_interfaces_config',
                    'yaml_file': '/path/to/file.yaml',
                    'py_file': '/path/to/file.py'
                }
                or None if skipped
        """
        # Build action name
        action_name = self._build_action_name(device_name, module_name, container_path)

        # Extract list key information if available
        list_info = container_data.get("list_info", {})
        raw_list_keys = list_info.get("keys", [])

        # Handle duplicate list key names by renaming them
        # Example: /evpn-instances/evpn-instance[name=X]/pseudowires/pseudowire[name=Y]
        # Rename to: evpn_instance_name, pseudowire_name
        list_keys_renamed = self._rename_duplicate_list_keys(
            raw_list_keys, container_path
        )
        list_key_names = {key["name"] for key in list_keys_renamed}

        # Extract parameters from container paths (excluding list keys)
        parameters = self._extract_parameters(container_data["paths"], list_key_names)

        # Skip if no parameters AND no list keys
        # (containers with only list keys should still generate actions)
        has_list_keys = bool(list_key_names)
        if not parameters and not has_list_keys:
            return None

        # Extract container type metadata
        is_writable = container_data.get("is_writable", True)
        container_type = container_data.get("container_type", "config")
        supported_operations = container_data.get(
            "supported_operations", ["get", "update", "replace", "delete"]
        )

        # Build container description
        description = self._build_description(module_name, container_path, is_writable)

        # Build class name (PascalCase for Python class)
        class_name = self._build_class_name(action_name)

        # Render templates
        timestamp = datetime.utcnow().isoformat() + "Z"

        is_writable = container_data.get("is_writable", True)
        container_type = container_data.get("container_type", "config")
        supported_operations = container_data.get(
            "supported_operations", ["get", "update", "replace", "delete"]
        )

        # Use list key information already extracted and renamed above
        has_list_keys = bool(list_keys_renamed)
        list_path = list_info.get("list_path", "")

        template_context = {
            "action_name": action_name,
            "pack_name": pack_name,
            "device": device_name,
            "module": module_name,
            "container_path": container_path,
            "description": description,
            "parameters": parameters,
            "class_name": class_name,
            "generated_timestamp": timestamp,
            "is_writable": is_writable,
            "container_type": container_type,
            "supported_operations": supported_operations,
            "has_list_keys": has_list_keys,
            "list_keys": list_keys_renamed,
            "list_path": list_path,
        }

        # Render YAML
        yaml_template = self.jinja_env.get_template("generated_action.yaml.j2")
        yaml_content = yaml_template.render(template_context)

        # Render Python
        py_template = self.jinja_env.get_template("generated_action.py.j2")
        py_content = py_template.render(template_context)

        # Write files
        yaml_file = os.path.join(self.output_dir, f"{action_name}.yaml")
        py_file = os.path.join(self.output_dir, f"{action_name}.py")

        with open(yaml_file, "w") as f:
            f.write(yaml_content)

        with open(py_file, "w") as f:
            f.write(py_content)

        # Make Python file executable
        os.chmod(py_file, 0o755)

        return {
            "success": True,
            "action_name": action_name,
            "module": module_name,
            "container": container_path,
            "param_count": len(parameters),
            "yaml_file": yaml_file,
            "py_file": py_file,
        }

    def _build_action_name(self, device_name, module_name, container_path):
        """
        Build clean action name following naming conventions
        Automatically handles long filenames that exceed filesystem limits

        Args:
        device_name: Device identifier
        module_name: YANG module name
        container_path: Container path

        Returns:
        str: Action name (e.g., 'device_192_168_1_50_openconfig_interfaces_config')

        Examples:
        ('192.168.1.50', 'openconfig-interfaces', '/interfaces/interface/config')
        -> 'device_192_168_1_50_openconfig_interfaces_interfaces_interface_config'

        (Very long path with deep nesting)
        -> 'device_192_168_1_50_openconfig_a1b2c3d4e5f6_config'
        """
        # Clean device name
        clean_device = device_name.replace(".", "_").replace("-", "_")

        # Clean module name (remove version suffix like @2021-01-01)
        clean_module = module_name.split("@")[0].replace("-", "_")

        # Extract container name from path
        # '/interfaces/interface/config' -> 'interfaces_interface_config'
        container_parts = container_path.strip("/").split("/")
        clean_container = "_".join(container_parts)

        # Build full name
        action_name = f"device_{clean_device}_{clean_module}_{clean_container}"

        # Ensure valid identifier (remove any remaining special chars)
        action_name = re.sub(r"[^a-zA-Z0-9_]", "_", action_name)

        # Remove consecutive underscores
        action_name = re.sub(r"_+", "_", action_name)

        # Handle filesystem filename length limit (255 chars max)
        # Leave margin for .yaml/.py extension and safety
        MAX_FILENAME_LENGTH = 200

        if len(action_name) > MAX_FILENAME_LENGTH:
            import hashlib

            # Create short hash of the full name for uniqueness
            hash_part = hashlib.md5(action_name.encode()).hexdigest()[:12]

            # Keep the most meaningful parts:
            # - Device identifier (always keep)
            # - Module name (abbreviated)
            # - Hash (for uniqueness)
            # - Last 2-3 parts of container path (most specific)

            device_prefix = f"device_{clean_device}"

            # Take first word of module name
            module_short = clean_module.split("_")[0]

            # Take last 2 parts of container path
            container_suffix = (
                "_".join(container_parts[-2:])
                if len(container_parts) >= 2
                else clean_container
            )

            # Build shortened name: device_X_module_hash_container_end
            action_name = (
                f"{device_prefix}_{module_short}_{hash_part}_{container_suffix}"
            )

            # Clean any remaining special chars from the shortened name
            action_name = re.sub(r"[^a-zA-Z0-9_]", "_", action_name)
            action_name = re.sub(r"_+", "_", action_name)

            # Final safety check - if still too long, just use hash
            if len(action_name) > MAX_FILENAME_LENGTH:
                action_name = f"{device_prefix}_{hash_part}"

        return action_name.lower()

    def _build_class_name(self, action_name):
        """
        Build Python class name from action name (PascalCase)

        Args:
            action_name: Action name (snake_case)

        Returns:
            str: Class name (PascalCase)

        Example:
            'device_192_168_1_50_openconfig_interfaces_config'
            -> 'Device192168150OpenconfigInterfacesConfig'
        """
        parts = action_name.split("_")
        return "".join(word.capitalize() for word in parts)

    def _build_description(self, module_name, container_path, is_writable=True):
        """
        Build human-readable description for action with operation hint

        Args:
            module_name: YANG module name
            container_path: Container path
            is_writable: Whether container is writable (default: True)

        Returns:
            str: Description with operation hint

        Examples:
            (writable=True)  -> "Configure Openconfig Interfaces - Interface Config"
            (writable=False) -> "Query Openconfig Interfaces - Interface State"
        """
        # Try to make it readable
        module_readable = module_name.replace("-", " ").replace("_", " ").title()

        # Get last part of path for context
        path_parts = container_path.strip("/").split("/")
        context = " ".join(path_parts[-2:]).replace("-", " ").title()

        # Use appropriate verb based on writability
        operation_verb = "Configure" if is_writable else "Query"

        return f"{operation_verb} {module_readable} - {context}"

    def _extract_parameters(self, paths, list_key_names=None):
        """
        Extract StackStorm parameters from YANG paths

        Handles parameter name conflicts with StackStorm runner built-ins
        by prefixing reserved names with 'yang_'

        Args:
            paths: Dict of {full_path: metadata}
            list_key_names: Set of list key parameter names to exclude

        Returns:
            dict: {param_name: param_spec}

        Example:
            {
                '/interfaces/interface/config/mtu': {'type': 'uint16', 'range': {...}},
                '/interfaces/interface/config/enabled': {'type': 'boolean'}
            }
            ->
            {
                'mtu': {'type': 'integer', 'minimum': 0, 'maximum': 65535},
                'enabled': {'type': 'boolean'}
            }

            If path contains 'timeout', it becomes 'yang_timeout' to avoid
            conflict with python-script runner's timeout parameter.
        """
        # Reserved parameter names from python-script runner
        # These cannot be overridden by action parameters
        RESERVED_PARAMS = {
            "timeout",  # Action execution timeout
            "sudo",  # Run with sudo
            "env",  # Environment variables
            "cwd",  # Working directory
            "kwarg_op",  # Keyword argument operator
        }

        parameters = {}
        list_key_names = list_key_names or set()

        for full_path, metadata in paths.items():
            # Extract leaf name (last part of path)
            leaf_name = full_path.strip("/").split("/")[-1]

            # Convert YANG dashes to underscores for valid parameter names
            param_name = leaf_name.replace("-", "_")

            # Skip if this parameter is a list key (will be added separately)
            if param_name in list_key_names:
                continue

            # Handle reserved parameter name conflicts
            if param_name in RESERVED_PARAMS:
                original_name = param_name
                param_name = f"yang_{param_name}"
                # Note: We could log this, but it would be too verbose
                # self.logger.debug(f"Renamed '{original_name}' -> '{param_name}' to avoid runner conflict")

            # Map YANG metadata to ST2 parameter spec
            param_spec = self.type_mapper.map_yang_to_st2_parameter(metadata)

            parameters[param_name] = param_spec

        return parameters

    def _rename_duplicate_list_keys(self, list_keys, container_path):
        """
        Rename duplicate list key names to avoid conflicts

        Example: /evpn-instances/evpn-instance[name]/pseudowires/pseudowire[name]
        Both have "name" as key, so rename based on list container:
        - evpn_instance_name
        - pseudowire_name
        """
        if len(list_keys) <= 1:
            return list_keys

        # Check for duplicates
        key_names = [key["name"] for key in list_keys]
        if len(key_names) == len(set(key_names)):
            # No duplicates
            return list_keys

        # Rename duplicates based on their list container name
        renamed_keys = []
        for key in list_keys:
            if key_names.count(key["name"]) > 1:
                # Duplicate - extract list container name from list_path
                # /network-instances/network-instance → network_instance
                # /vlans/vlan → vlan
                list_path = key.get("list_path", "")
                if list_path:
                    list_container = list_path.rstrip("/").split("/")[-1]
                    list_container_clean = list_container.replace("-", "_")
                    new_name = f"{list_container_clean}_{key['name']}"

                    renamed_key = key.copy()
                    renamed_key["name"] = new_name
                    renamed_key["original_name"] = key["name"]
                    renamed_keys.append(renamed_key)
                else:
                    renamed_keys.append(key)
            else:
                renamed_keys.append(key)

        return renamed_keys
