#!/usr/bin/env python3
"""
Container Grouper - Group YANG paths by logical containers
Organizes paths into actionable containers (e.g., /interfaces/interface/config)
"""
class ContainerGrouper:
    """Group YANG paths into logical containers for action generation"""
    
    def __init__(self, yang_schema):
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
    
    def group_by_container(self, config_only=True, min_params=1):
        """
        Group paths by their parent container
        
        Args:
            config_only: Only include config=true paths (default: True)
            min_params: Minimum parameters to create action (default: 1)
        
        Returns:
            dict: Grouped containers
                {
                    'module_name': {
                        '/container/path': {
                            'paths': {leaf_path: metadata, ...},
                            'param_count': N
                        }
                    }
                }
        """
        grouped = {}
        
        for module_name, module_data in self.yang_schema.items():
            # Extract paths from wrapper structure
            paths = module_data.get('paths', {})
            if not paths:
                continue
            
            module_containers = {}
            
            for path, metadata in paths.items():
                # Filter by config status
                if config_only and not metadata.get('config', True):
                    continue
                
                # Skip read-only paths
                if config_only and metadata.get('readonly', False):
                    continue
                
                # Find parent container
                container_path = self._get_container_path(path)
                if not container_path:
                    continue
                
                # Initialize container group
                if container_path not in module_containers:
                    module_containers[container_path] = {
                        'paths': {},
                        'param_count': 0
                    }
                
                # Add path to container
                module_containers[container_path]['paths'][path] = metadata
                module_containers[container_path]['param_count'] += 1
            
            # Filter out containers with too few parameters
            filtered_containers = {
                path: data for path, data in module_containers.items()
                if data['param_count'] >= min_params
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
        parts = full_path.strip('/').split('/')
        
        # Need at least 2 parts (container + leaf)
        if len(parts) < 2:
            return None
        
        # Skip paths with list keys (contain brackets)
        if '[' in full_path or ']' in full_path:
            return None
        
        # Container is everything except the last part (the leaf)
        container_parts = parts[:-1]
        
        # Prefer containers ending with 'config' or 'state'
        # This follows OpenConfig/IETF conventions
        container_path = '/' + '/'.join(container_parts)
        
        return container_path
    
    def get_container_summary(self, grouped_containers):
        """
        Get summary statistics of grouped containers
        
        Args:
            grouped_containers: Output from group_by_container()
        
        Returns:
            dict: Summary statistics
        """
        total_modules = len(grouped_containers)
        total_containers = sum(len(containers) for containers in grouped_containers.values())
        total_paths = sum(
            data['param_count']
            for module_data in grouped_containers.values()
            for data in module_data.values()
        )
        
        return {
            'total_modules': total_modules,
            'total_containers': total_containers,
            'total_paths': total_paths,
            'avg_paths_per_container': total_paths / total_containers if total_containers > 0 else 0
        }