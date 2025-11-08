#!/usr/bin/env python3
"""Generate StackStorm Actions from Parsed YANG Models"""
from st2common.runners.base_action import Action
import os
import sys
import json
import time
import subprocess
# Add lib to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lib'))
from pack_utils import generate_pack_name, get_pack_base_dir
from container_grouper import ContainerGrouper
from action_generator import ActionGenerator
class YangGenerateActionsAction(Action):
    
    def run(self, device_name, action_prefix=None, max_actions=0,
           register_actions=True, output_pack=None):
        """
        Generate StackStorm actions from parsed YANG schema
        
        Args:
            device_name: Device to generate actions for
            action_prefix: Prefix for action names (default: device name)
            max_actions: Maximum actions to generate 
            register_actions: Auto-register after generation (default: True)
            output_pack: Target pack (default: device pack)
        
        Returns:
            tuple: (success, result_dict)
        """
        start_time = time.time()
        
        # Use device name for action prefix if not specified
        if not action_prefix:
            action_prefix = device_name
        
        # Determine target pack
        if not output_pack:
            output_pack = generate_pack_name(device_name)
        
        self.logger.info(f"Generating actions for device: {device_name}")
        self.logger.info(f"Action prefix: {action_prefix}")
        self.logger.info(f"Target pack: {output_pack}")
        
        try:
            # Load parsed YANG data from datastore
            self.logger.info("Loading YANG schema from datastore...")
            key = f"gnmi_toolkit.YangParseModelsAction:device:{device_name}:yang_paths"
            schema_json = self.action_service.get_value(
                name=key,
                local=False,  # Don't add action class prefix
                decrypt=False
            )
            
            if not schema_json:
                return (False, {
                    'success': False,
                    'error': f"No YANG schema found for device: {device_name}",
                    'hint': 'Run yang_parse_models first'
                })
            
            yang_schema = json.loads(schema_json)
            
            total_modules = len(yang_schema)
            total_paths = sum(data['path_count'] for data in yang_schema.values())
            
            self.logger.info(f"Loaded schema: {total_modules} modules, {total_paths} paths")
            
            # Setup paths
            pack_base_dir = get_pack_base_dir(device_name)
            template_dir = os.path.join(os.path.dirname(__file__), 'templates')
            output_dir = os.path.join(pack_base_dir, 'actions')
            
            # Create pack structure if needed
            self._ensure_pack_structure(pack_base_dir, output_pack, device_name)
            
            self.logger.info(f"Output directory: {output_dir}")
            
            # Group paths by container
            self.logger.info("Grouping paths into containers...")
            grouper = ContainerGrouper(yang_schema)
            grouped = grouper.group_by_container(config_only=False, min_params=1)
            
            summary = grouper.get_container_summary(grouped)
            self.logger.info(
                f"Found {summary['total_containers']} containers "
                f"in {summary['total_modules']} modules"
            )
            
            # Initialize generator
            generator = ActionGenerator(template_dir, output_dir)
            
            # Generate actions
            self.logger.info("Generating actions...")
            generated_actions = []
            action_count = 0
            
            for module_name, containers in grouped.items():
                for container_path, container_data in containers.items():
                    # Check max actions limit (0 means unlimited)
                    if max_actions > 0 and action_count >= max_actions:
                        self.logger.info(f"Reached max_actions limit: {max_actions}")
                        break
                    
                    # Generate action
                    result = generator.generate_action_for_container(
                        device_name=action_prefix,
                        module_name=module_name,
                        container_path=container_path,
                        container_data=container_data,
                        pack_name=output_pack
                    )
                    
                    if result and result['success']:
                        generated_actions.append(result)
                        action_count += 1
                        
                        # Log progress every 10 actions
                        if action_count % 10 == 0:
                            self.logger.info(f"Generated {action_count} actions...")
                
                # Break outer loop if limit reached
                if max_actions > 0 and action_count >= max_actions:
                    self.logger.info(f"Reached max_actions limit: {max_actions}")
                    break
            
            generation_time = time.time() - start_time
            self.logger.info(
                f"Generation complete: {len(generated_actions)} actions "
                f"in {generation_time:.2f}s"
            )
            
            # Register actions with StackStorm
            if register_actions and generated_actions:
                self.logger.info("Registering actions with StackStorm...")
                try:
                    result = subprocess.run(
                        ['st2ctl', 'reload', f'--register-pack={output_pack}'],
                        capture_output=True,
                        text=True,
                        timeout=30
                    )
                    
                    if result.returncode == 0:
                        self.logger.info("Actions registered successfully")
                    else:
                        self.logger.warning(f"Registration failed: {result.stderr}")
                
                except Exception as e:
                    self.logger.warning(f"Failed to register actions: {str(e)}")
            
            # Build summary
            total_time = time.time() - start_time
            
            return (True, {
                'success': True,
                'device_name': device_name,
                'pack_name': output_pack,
                'generated_count': len(generated_actions),
                'actions': [
                    {
                        'name': a['action_name'],
                        'module': a['module'],
                        'container': a['container'],
                        'params': a['param_count']
                    }
                    for a in generated_actions[:20]  # First 20 for output
                ],
                'total_modules': summary['total_modules'],
                'total_containers': summary['total_containers'],
                'generation_time_seconds': round(generation_time, 2),
                'total_time_seconds': round(total_time, 2),
                'registered': register_actions
            })
        
        except json.JSONDecodeError as e:
            self.logger.error("Failed to parse YANG schema from datastore")
            return (False, {
                'success': False,
                'error': 'Invalid JSON in datastore',
                'details': str(e)
            })
        
        except Exception as e:
            self.logger.error("Action generation failed")
            self.logger.error(f"Error: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return (False, {
                'success': False,
                'error': str(e),
                'traceback': traceback.format_exc()
            })
    
    def _ensure_pack_structure(self, pack_dir, pack_name, device_name):
        """
        Ensure device pack has proper structure
        
        Args:
            pack_dir: Pack base directory
            pack_name: Pack name
            device_name: Device identifier
        """
        # Create directories
        os.makedirs(os.path.join(pack_dir, 'actions', 'generated'), exist_ok=True)
        
        # Create pack.yaml if it doesn't exist
        pack_yaml_path = os.path.join(pack_dir, 'pack.yaml')
        if not os.path.exists(pack_yaml_path):
            # Build pack.yaml content with proper indentation
            pack_yaml_content = [
                "---",
                f"name: {pack_name}",
                f"description: \"Auto-generated pack for device {device_name}\"",
                "version: \"0.1.0\"",
                "author: \"gnmi_toolkit\"",
                "email: \"admin@example.com\"",
                "keywords:",
                "  - network",
                "  - gnmi",
                "  - yang",
                "  - device",
                ""  # Trailing newline
            ]
            
            with open(pack_yaml_path, 'w') as f:
                f.write('\n'.join(pack_yaml_content))
            
            self.logger.info(f"Created pack.yaml for {pack_name}")