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
            loader=FileSystemLoader(template_dir),
            trim_blocks=True,
            lstrip_blocks=True
        )
    
    def generate_action_for_container(self, device_name, module_name, 
                                     container_path, container_data, pack_name):
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
        
        # Extract parameters from container paths
        parameters = self._extract_parameters(container_data['paths'])
        
        # Skip if no parameters (shouldn't happen with min_params filter)
        if not parameters:
            return None
        
        # Build container description
        description = self._build_description(module_name, container_path)
        
        # Build class name (PascalCase for Python class)
        class_name = self._build_class_name(action_name)
        
        # Render templates
        timestamp = datetime.utcnow().isoformat() + 'Z'
        
        template_context = {
            'action_name': action_name,
            'pack_name': pack_name,
            'device': device_name,
            'module': module_name,
            'container_path': container_path,
            'description': description,
            'parameters': parameters,
            'class_name': class_name,
            'generated_timestamp': timestamp
        }
        
        # Render YAML
        yaml_template = self.jinja_env.get_template('generated_action.yaml.j2')
        yaml_content = yaml_template.render(template_context)
        
        # Render Python
        py_template = self.jinja_env.get_template('generated_action.py.j2')
        py_content = py_template.render(template_context)
        
        # Write files
        yaml_file = os.path.join(self.output_dir, f"{action_name}.yaml")
        py_file = os.path.join(self.output_dir, f"{action_name}.py")
        
        with open(yaml_file, 'w') as f:
            f.write(yaml_content)
        
        with open(py_file, 'w') as f:
            f.write(py_content)
        
        # Make Python file executable
        os.chmod(py_file, 0o755)
        
        return {
            'success': True,
            'action_name': action_name,
            'module': module_name,
            'container': container_path,
            'param_count': len(parameters),
            'yaml_file': yaml_file,
            'py_file': py_file
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
        clean_device = device_name.replace('.', '_').replace('-', '_')

        # Clean module name (remove version suffix like @2021-01-01)
        clean_module = module_name.split('@')[0].replace('-', '_')

        # Extract container name from path
        # '/interfaces/interface/config' -> 'interfaces_interface_config'
        container_parts = container_path.strip('/').split('/')
        clean_container = '_'.join(container_parts)

        # Build full name
        action_name = f"device_{clean_device}_{clean_module}_{clean_container}"

        # Ensure valid identifier (remove any remaining special chars)
        action_name = re.sub(r'[^a-zA-Z0-9_]', '_', action_name)

        # Remove consecutive underscores
        action_name = re.sub(r'_+', '_', action_name)

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
            module_short = clean_module.split('_')[0]

            # Take last 2 parts of container path
            container_suffix = '_'.join(container_parts[-2:]) if len(container_parts) >= 2 else clean_container

            # Build shortened name: device_X_module_hash_container_end
            action_name = f"{device_prefix}_{module_short}_{hash_part}_{container_suffix}"
            
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
        parts = action_name.split('_')
        return ''.join(word.capitalize() for word in parts)
    
    def _build_description(self, module_name, container_path):
        """
        Build human-readable description for action
        
        Args:
            module_name: YANG module name
            container_path: Container path
        
        Returns:
            str: Description
        """
        # Try to make it readable
        module_readable = module_name.replace('-', ' ').replace('_', ' ').title()
        
        # Get last part of path for context
        path_parts = container_path.strip('/').split('/')
        context = ' '.join(path_parts[-2:]).replace('-', ' ').title()
        
        return f"Configure {module_readable} - {context}"
    
    def _extract_parameters(self, paths):
        """
        Extract StackStorm parameters from YANG paths

        Handles parameter name conflicts with StackStorm runner built-ins
        by prefixing reserved names with 'yang_'

        Args:
            paths: Dict of {full_path: metadata}

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
            'timeout',   # Action execution timeout
            'sudo',      # Run with sudo
            'env',       # Environment variables
            'cwd',       # Working directory
            'kwarg_op',  # Keyword argument operator
        }

        parameters = {}

        for full_path, metadata in paths.items():
            # Extract leaf name (last part of path)
            leaf_name = full_path.strip('/').split('/')[-1]
            
            # Convert YANG dashes to underscores for valid parameter names
            param_name = leaf_name.replace('-', '_')
            
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