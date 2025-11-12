#!/usr/bin/env python3
"""YANG Parse Models Action"""
from st2common.runners.base_action import Action
import os
import sys
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add lib to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "lib"))
from yang_parser import YangParser
from ast_walker import ASTWalker
from pack_utils import get_yang_models_path, generate_pack_name


class YangParseModelsAction(Action):

    def run(self, device_name, yang_path=None, workers=10, store_in_datastore=True):
        """Parse YANG models using pyang library"""

        start_time = time.time()

        # Setup path - reads from device pack directory
        if not yang_path:
            yang_path = get_yang_models_path(device_name)

        pack_name = generate_pack_name(device_name)
        self.logger.info(f"Parsing YANG models for device: {device_name}")
        self.logger.info(f"Device pack: {pack_name}")
        self.logger.info(f"YANG path: {yang_path}")

        try:
            # Load YANG modules
            self.logger.info("Loading YANG modules...")
            load_start = time.time()

            parser = YangParser(yang_path)
            modules = parser.load_modules()

            load_time = time.time() - load_start
            self.logger.info(f"Loaded {len(modules)} YANG modules in {load_time:.2f}s")

            # Extract paths from modules
            self.logger.info(f"Extracting paths using {workers} concurrent workers...")
            parse_start = time.time()

            path_catalog, list_registry = self._parse_modules_concurrent(
                modules, workers
            )

            parse_time = time.time() - parse_start
            total_paths = sum(data["path_count"] for data in path_catalog.values())

            self.logger.info(
                f"Extracted {total_paths} paths from {len(path_catalog)} modules in {parse_time:.2f}s"
            )

            # Log statistics
            self._log_parse_statistics(path_catalog)

            # Store in datastore
            if store_in_datastore and total_paths > 0:
                self.logger.info("Storing in datastore...")
                key = f"device:{device_name}:yang_paths"
                self.action_service.set_value(
                    key, json.dumps(path_catalog), ttl=None, encrypt=False
                )
                self.logger.info(f"Stored in datastore with key: {key}")

            # Store list registry in datastore
            if store_in_datastore and list_registry:
                total_lists = sum(len(lists) for lists in list_registry.values())
                self.logger.info(
                    f"Storing {total_lists} lists from {len(list_registry)} modules..."
                )
                key_lists = f"device:{device_name}:yang_lists"
                self.action_service.set_value(
                    key_lists, json.dumps(list_registry), ttl=None, encrypt=False
                )
                self.logger.info(f"Stored list registry with key: {key_lists}")

            # Summary
            total_time = time.time() - start_time
            self.logger.info(
                f"Parsing complete: {len(path_catalog)} modules, "
                f"{total_paths} paths, {total_time:.2f}s total"
            )

            # Build result
            sample_modules = self._build_sample_output(path_catalog)

            return (
                True,
                {
                    "success": True,
                    "device_name": device_name,
                    "modules_parsed": len(path_catalog),
                    "total_paths": total_paths,
                    "load_time_seconds": round(load_time, 2),
                    "parse_time_seconds": round(parse_time, 2),
                    "total_time_seconds": round(total_time, 2),
                    "sample_modules": sample_modules,
                    "stored_in_datastore": store_in_datastore,
                },
            )

        except FileNotFoundError as e:
            self.logger.error("YANG path not found")
            self.logger.error(f"Error: {str(e)}")
            self.logger.error(
                "Hint: Run yang_download_models first to download YANG files"
            )
            return (
                False,
                {
                    "success": False,
                    "error": str(e),
                    "hint": "Run yang_download_models first to download YANG files",
                },
            )

        except Exception as e:
            self.logger.error("YANG parsing failed")
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

    def _parse_modules_concurrent(self, modules, workers):
        """
        Parse YANG modules concurrently using ThreadPoolExecutor

        Uses concurrent processing to speed up parsing of multiple modules.
        Each module is parsed independently in its own thread.

        Args:
            modules: Dict of {module_name: pyang_module}
            workers: Number of concurrent worker threads

        Returns:
            tuple: (path_catalog, list_registry_all)
                - path_catalog: Dict of {module_name: {'paths': {...}, 'path_count': N}}
                - list_registry_all: Dict of {module_name: {list_path: metadata}}
        """
        path_catalog = {}
        list_registry_all = {}  # NEW

        def parse_module(module, module_name):
            walker = ASTWalker()
            paths = walker.extract_paths(module)
            list_registry = walker.get_list_registry()
            return (paths, list_registry)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(parse_module, module, module_name): module_name
                for module_name, module in modules.items()
            }

            completed = 0
            total = len(modules)

            for future in as_completed(futures):
                module_name = futures[future]
                completed += 1

                try:
                    paths, list_registry = future.result()
                    if paths:
                        path_catalog[module_name] = {
                            "paths": paths,
                            "path_count": len(paths),
                        }

                        if list_registry:
                            list_registry_all[module_name] = list_registry

                        if completed % 10 == 0:
                            success_count = len(path_catalog)
                            lists_count = len(list_registry_all)
                            self.logger.info(
                                f"Progress: {completed}/{total} modules "
                                f"({success_count} with paths, {lists_count} with lists)"
                            )

                except Exception as e:
                    self.logger.warning(f"Failed to parse {module_name}: {str(e)}")

        return path_catalog, list_registry_all

    def _log_parse_statistics(self, path_catalog):
        """
        Log detailed statistics about parsed YANG data

        Calculates and logs:
        - Total modules and paths
        - Config vs state path breakdown
        - Average paths per module
        - Top 10 modules by path count
        - Validation metadata counts (enums, ranges)

        Args:
            path_catalog: Dict of {module_name: {'paths': {...}, 'path_count': N}}
        """

        # Count statistics
        total_modules = len(path_catalog)
        total_paths = sum(data["path_count"] for data in path_catalog.values())

        # Count config vs state paths
        config_count = sum(
            1
            for mod_data in path_catalog.values()
            for path_info in mod_data["paths"].values()
            if path_info.get("config", True)
        )
        state_count = total_paths - config_count

        # Log statistics
        self.logger.info(
            f"Statistics: {total_modules} modules, {total_paths} paths "
            f"({config_count} config, {state_count} state)"
        )

        # Calculate average
        if total_modules > 0:
            avg_paths = total_paths / total_modules
            self.logger.info(f"Average paths per module: {avg_paths:.1f}")

        # Find and log top modules by path count
        modules_by_path_count = sorted(
            path_catalog.items(), key=lambda x: x[1]["path_count"], reverse=True
        )

        self.logger.info("Top 10 modules by path count:")
        for i, (module_name, data) in enumerate(modules_by_path_count[:10], 1):
            self.logger.info(
                f"  {i:2d}. {module_name:40s} - {data['path_count']:4d} paths"
            )

        # Log paths with enums for validation features
        enum_count = sum(
            1
            for mod_data in path_catalog.values()
            for path_info in mod_data["paths"].values()
            if "enum" in path_info
        )
        range_count = sum(
            1
            for mod_data in path_catalog.values()
            for path_info in mod_data["paths"].values()
            if "range" in path_info
        )

        if enum_count > 0 or range_count > 0:
            self.logger.info(
                f"Validation metadata: {enum_count} enums, {range_count} ranges"
            )

    def _build_sample_output(self, path_catalog):
        """
        Build sample output showing first 5 modules and their paths

        Creates a condensed view of parsed data for action result output,
        showing the first 5 modules with up to 5 sample paths each.

        Args:
            path_catalog: Dict of {module_name: {'paths': {...}, 'path_count': N}}

        Returns:
            list: Sample modules with format:
                [
                    {
                        'module': 'openconfig-interfaces',
                        'path_count': 42,
                        'sample_paths': [
                            {'path': '/interfaces/interface/config/enabled', 'type': 'boolean', ...},
                            ...
                        ]
                    },
                    ...
                ]
        """
        sample_modules = []
        for mod_name, mod_data in list(path_catalog.items())[:5]:
            # Get sample paths
            sample_paths = []
            for path, info in list(mod_data["paths"].items())[:5]:
                sample_paths.append(
                    {
                        "path": path,
                        "type": info.get("type", "unknown"),
                        "has_enum": "enum" in info,
                        "has_range": "range" in info,
                    }
                )

            sample_modules.append(
                {
                    "module": mod_name,
                    "path_count": mod_data["path_count"],
                    "sample_paths": sample_paths,
                }
            )

        return sample_modules
