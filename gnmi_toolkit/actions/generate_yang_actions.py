#!/usr/bin/env python3
"""Generate StackStorm Actions from Parsed YANG Models"""
from st2common.runners.base_action import Action
import os
import sys
import json
import time
import subprocess

# Add lib to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "lib"))
from pack_utils import generate_pack_name, get_pack_base_dir
from container_grouper import ContainerGrouper
from action_generator import ActionGenerator


class YangGenerateActionsAction(Action):

    def run(
        self,
        device_name,
        action_prefix=None,
        max_actions=0,
        register_actions=True,
        output_pack=None,
        setup_virtualenv=True,
    ):
        """
        Generate StackStorm actions from parsed YANG schema

        Args:
            device_name: Device to generate actions for
            action_prefix: Prefix for action names (default: device name)
            max_actions: Maximum actions to generate
            register_actions: Auto-register after generation (default: True)
            output_pack: Target pack (default: device pack)
            setup_virtualenv: Setup virtualenv if needed (default: True)

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
                name=key, local=False, decrypt=False  # Don't add action class prefix
            )

            if not schema_json:
                return (
                    False,
                    {
                        "success": False,
                        "error": f"No YANG schema found for device: {device_name}",
                        "hint": "Run yang_parse_models first",
                    },
                )

            yang_schema = json.loads(schema_json)

            # Load list registry from datastore
            key_lists = (
                f"gnmi_toolkit.YangParseModelsAction:device:{device_name}:yang_lists"
            )
            lists_json = self.action_service.get_value(
                name=key_lists, local=False, decrypt=False
            )
            list_registry = json.loads(lists_json) if lists_json else {}

            if list_registry:
                total_lists = sum(len(lists) for lists in list_registry.values())
                self.logger.info(
                    f"Loaded list registry: {total_lists} lists from {len(list_registry)} modules"
                )
            else:
                self.logger.info("No list registry found (lists will be skipped)")

            total_modules = len(yang_schema)
            total_paths = sum(data["path_count"] for data in yang_schema.values())

            self.logger.info(
                f"Loaded schema: {total_modules} modules, {total_paths} paths"
            )

            # Setup paths
            pack_base_dir = get_pack_base_dir(device_name)
            template_dir = os.path.join(os.path.dirname(__file__), "templates")
            output_dir = os.path.join(pack_base_dir, "actions")

            # Create pack structure if needed
            self._ensure_pack_structure(pack_base_dir, output_pack, device_name)

            self.logger.info(f"Output directory: {output_dir}")

            # Group paths by container
            self.logger.info("Grouping paths into containers...")
            grouper = ContainerGrouper(yang_schema, list_registry)
            grouped = grouper.group_by_container(min_params=1)

            summary = grouper.get_container_summary(grouped)
            self.logger.info(
                f"Found {summary['total_containers']} containers "
                f"({summary['config_containers']} config, {summary['state_containers']} state) "
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
                        pack_name=output_pack,
                    )

                    if result and result["success"]:
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

            # Setup virtual environment (MUST come before registration)
            venv_result = {"success": True, "skipped": True}
            if setup_virtualenv:
                venv_result = self._setup_virtualenv(output_pack, pack_base_dir)

                if not venv_result["success"] and not venv_result["skipped"]:
                    self.logger.warning(
                        f"Virtualenv setup failed: {venv_result['message']}. "
                        f"Manual setup: st2 run packs.setup_virtualenv packs={output_pack}"
                    )

            # Register actions with StackStorm (comes AFTER virtualenv)
            registration_result = {"success": True, "action_count": 0}
            if register_actions and generated_actions:
                registration_result = self._register_pack(output_pack)

                if not registration_result["success"]:
                    self.logger.warning(
                        f"Registration failed: {registration_result['message']}. "
                        f"Manual registration: st2 run packs.load packs={output_pack} register=actions"
                    )

            # Build summary
            total_time = time.time() - start_time

            return (
                True,
                {
                    "success": True,
                    "device_name": device_name,
                    "pack_name": output_pack,
                    "generated_count": len(generated_actions),
                    "actions": [
                        {
                            "name": a["action_name"],
                            "module": a["module"],
                            "container": a["container"],
                            "params": a["param_count"],
                        }
                        for a in generated_actions[:20]  # First 20 for output
                    ],
                    "total_modules": summary["total_modules"],
                    "total_containers": summary["total_containers"],
                    "generation_time_seconds": round(generation_time, 2),
                    "total_time_seconds": round(total_time, 2),
                    # Virtualenv info
                    "virtualenv_setup": venv_result["success"],
                    "virtualenv_skipped": venv_result.get("skipped", False),
                    "virtualenv_message": venv_result.get("message", ""),
                    # Registration info
                    "registered": registration_result["success"],
                    "registered_action_count": registration_result.get("action_count", 0),
                    "registration_message": registration_result.get("message", ""),
                },
            )

        except json.JSONDecodeError as e:
            self.logger.error("Failed to parse YANG schema from datastore")
            return (
                False,
                {
                    "success": False,
                    "error": "Invalid JSON in datastore",
                    "details": str(e),
                },
            )

        except Exception as e:
            self.logger.error("Action generation failed")
            self.logger.error(f"Error: {str(e)}")
            import traceback

            self.logger.error(traceback.format_exc())
            return (
                False,
                {
                    "success": False,
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                },
            )

    def _ensure_pack_structure(self, pack_dir, pack_name, device_name):
        """
        Ensure device pack has proper structure

        Args:
            pack_dir: Pack base directory
            pack_name: Pack name
            device_name: Device identifier
        """
        # Create directories
        os.makedirs(os.path.join(pack_dir, "actions", "generated"), exist_ok=True)

        # Create pack.yaml if it doesn't exist
        pack_yaml_path = os.path.join(pack_dir, "pack.yaml")
        if not os.path.exists(pack_yaml_path):
            # Build pack.yaml content with proper indentation
            pack_yaml_content = [
                "---",
                f"name: {pack_name}",
                f'description: "Auto-generated pack for device {device_name}"',
                'version: "0.1.0"',
                'author: "gnmi_toolkit"',
                'email: "admin@example.com"',
                "keywords:",
                "  - network",
                "  - gnmi",
                "  - yang",
                "  - device",
                "",  # Trailing newline
            ]

            with open(pack_yaml_path, "w") as f:
                f.write("\n".join(pack_yaml_content))

            self.logger.info(f"Created pack.yaml for {pack_name}")

        # Create requirements.txt if it doesn't exist
        requirements_path = os.path.join(pack_dir, "requirements.txt")
        if not os.path.exists(requirements_path):
            requirements_content = [
                f"# Auto-generated requirements for pack: {pack_name}",
                "ncclient",
                "pygnmi",
                "jinja2",
                "",  # Trailing newline
            ]

            with open(requirements_path, "w") as f:
                f.write("\n".join(requirements_content))

            self.logger.info(f"Created requirements.txt for {pack_name}")
        else:
            self.logger.info(f"requirements.txt already exists for {pack_name}")

    def _setup_virtualenv(self, pack_name, pack_dir):
        """
        Setup virtual environment using StackStorm's packs.setup_virtualenv action

        Only creates virtualenv if:
        - It doesn't already exist
        - requirements.txt exists in pack

        Args:
            pack_name: Pack name (e.g., 'device_192_168_1_50')
            pack_dir: Pack base directory

        Returns:
            dict: {'success': bool, 'message': str, 'skipped': bool}
        """
        venv_path = f"/opt/stackstorm/virtualenvs/{pack_name}"
        requirements_path = os.path.join(pack_dir, "requirements.txt")

        # Check if requirements.txt exists
        if not os.path.exists(requirements_path):
            self.logger.warning(
                f"No requirements.txt found for {pack_name}. "
                "Skipping virtualenv setup."
            )
            return {
                "success": False,
                "message": "No requirements.txt found",
                "skipped": True,
            }

        # Check if virtualenv already exists
        if os.path.exists(venv_path) and os.path.isdir(venv_path):
            self.logger.info(
                f"Virtual environment already exists for {pack_name}"
            )
            return {
                "success": True,
                "message": "Virtual environment already exists",
                "skipped": True,
            }

        # Create virtualenv using StackStorm action
        self.logger.info(f"Setting up virtual environment for {pack_name}...")
        self.logger.info("This may take 1-2 minutes to install dependencies...")

        try:
            # Use st2 run to execute packs.setup_virtualenv action
            result = subprocess.run(
                ["st2", "run", "packs.setup_virtualenv", f"packs={pack_name}"],
                capture_output=True,
                text=True,
                timeout=300,  # 5 minutes
            )

            if result.returncode == 0:
                self.logger.info(
                    f"Virtual environment created successfully for {pack_name}"
                )
                return {
                    "success": True,
                    "message": "Virtual environment created successfully",
                    "skipped": False,
                }
            else:
                error_msg = result.stderr or result.stdout
                self.logger.error(f"Virtualenv setup failed: {error_msg}")
                return {
                    "success": False,
                    "message": f"Setup failed: {error_msg}",
                    "skipped": False,
                }

        except subprocess.TimeoutExpired:
            self.logger.error("Virtualenv setup timed out")
            return {
                "success": False,
                "message": "Setup timed out after 5 minutes",
                "skipped": False,
            }
        except Exception as e:
            self.logger.error(f"Virtualenv setup error: {str(e)}")
            return {
                "success": False,
                "message": f"Exception: {str(e)}",
                "skipped": False,
            }

    def _register_pack(self, pack_name):
        """
        Register pack actions using StackStorm's packs.load action

        Args:
            pack_name: Pack name to register

        Returns:
            dict: {'success': bool, 'message': str, 'action_count': int}
        """
        self.logger.info(f"Registering actions for pack: {pack_name}")

        try:
            # Use packs.load with register=actions for granular control
            result = subprocess.run(
                ["st2", "run", "packs.load", f"packs={pack_name}", "register=actions"],
                capture_output=True,
                text=True,
                timeout=60,
            )

            if result.returncode == 0:
                self.logger.info(f"Actions registered successfully for {pack_name}")

                # Try to parse action count from output
                action_count = 0
                if "actions:" in result.stdout:
                    try:
                        # Output format: "actions: 13"
                        import re

                        match = re.search(r"actions:\s*(\d+)", result.stdout)
                        if match:
                            action_count = int(match.group(1))
                    except:
                        pass

                return {
                    "success": True,
                    "message": "Actions registered successfully",
                    "action_count": action_count,
                }
            else:
                error_msg = result.stderr or result.stdout
                self.logger.error(f"Registration failed: {error_msg}")
                return {
                    "success": False,
                    "message": f"Registration failed: {error_msg}",
                    "action_count": 0,
                }

        except subprocess.TimeoutExpired:
            self.logger.error("Registration timed out")
            return {
                "success": False,
                "message": "Registration timed out after 60 seconds",
                "action_count": 0,
            }
        except Exception as e:
            self.logger.error(f"Registration error: {str(e)}")
            return {
                "success": False,
                "message": f"Exception: {str(e)}",
                "action_count": 0,
            }
