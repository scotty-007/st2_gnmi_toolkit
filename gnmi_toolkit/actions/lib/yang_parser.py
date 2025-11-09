#!/usr/bin/env python3

"""
YANG Parser - Pyang Library Interface
Loads YANG modules and creates pyang context for parsing
"""


from pyang import repository, context
import os


class YangParser:
    """Interface to pyang library for loading and managing YANG modules"""

    def __init__(self, yang_path):
        """
        Initialize parser with YANG files directory

        Args:
            yang_path: Path to directory containing .yang files
        """
        self.yang_path = yang_path
        self.repos = None
        self.ctx = None
        self.modules = {}

    def load_modules(self):
        """
        Load all YANG modules from directory into pyang context

        Returns:
            dict: {module_name: pyang_module_object}
        """
        if not os.path.exists(self.yang_path):
            raise FileNotFoundError(f"YANG path not found: {self.yang_path}")

        # Get all .yang files
        yang_files = [f[:-5] for f in os.listdir(self.yang_path) if f.endswith(".yang")]

        if not yang_files:
            raise ValueError(f"No .yang files found in {self.yang_path}")

        # Create pyang repository and context
        self.repos = repository.FileRepository(self.yang_path)
        self.ctx = context.Context(self.repos)

        # Add all modules to context (handles imports/dependencies)
        modules_loaded = {}
        failed_modules = []
        for module_name in yang_files:
            try:
                # Read the YANG file content
                file_path = os.path.join(self.yang_path, f"{module_name}.yang")
                with open(file_path, "r", encoding="utf-8") as f:
                    text = f.read()

                # Add module with text content
                module = self.ctx.add_module(file_path, text)
                if module:
                    modules_loaded[module_name] = module
            except Exception as e:
                # Log the error so we can see what's wrong
                failed_modules.append((module_name, str(e)))
        # Print first few failures for debugging
        if failed_modules and len(failed_modules) <= 10:
            print(f"Failed to load {len(failed_modules)} modules:")
            for mod, err in failed_modules[:10]:
                print(f"  {mod}: {err}")
        elif failed_modules:
            print(f"Failed to load {len(failed_modules)} modules (showing first 10):")
            for mod, err in failed_modules[:10]:
                print(f"  {mod}: {err}")

        # Validate context (resolves all references, typedefs, groupings)
        # This is CRITICAL for cross-module enum resolution!
        self.ctx.validate()

        self.modules = modules_loaded
        return modules_loaded

    def get_context(self):
        """Get pyang context (needed for type resolution)"""
        return self.ctx

    def get_module(self, module_name):
        """Get a specific loaded module"""
        return self.modules.get(module_name)

    def get_all_modules(self):
        """Get all loaded modules"""
        return self.modules
