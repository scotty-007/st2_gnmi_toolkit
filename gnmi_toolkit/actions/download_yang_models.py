#!/usr/bin/env python3
"""Download YANG models from network devices via NETCONF"""

from st2common.runners.base_action import Action
from ncclient import manager
import os
import sys
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add lib to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lib'))
from pack_utils import get_yang_models_path, generate_pack_name


class YangDownloadModelsAction(Action):
    
    def run(self, host, port=830, username=None, password=None, 
            storage_path=None, workers=10, max_retries=3, hostkey_verify=False):
        """Download all YANG models from device via NETCONF"""
        
        start_time = time.time()
        
        start_time = time.time()
        
        # Setup storage path - creates device pack directory structure
        if not storage_path:
            storage_path = get_yang_models_path(host)
        
        # Create directory structure (pack skeleton without metadata)
        os.makedirs(storage_path, exist_ok=True)
        
        pack_name = generate_pack_name(host)
        self.logger.info(f"Device pack: {pack_name}")
        self.logger.info(f"Storing YANG models in: {storage_path}")

        # Connect and get list of available schemas
        self.logger.info(f"Connecting to {host}:{port}...")
        try:
            with manager.connect(
                host=host,
                port=port,
                username=username,
                password=password,
                hostkey_verify=hostkey_verify,
                device_params={'name': 'default'}
            ) as m:
                # Get list of schemas from device
                schemas = m.server_capabilities
                module_list = []
                
                # Extract module names from capabilities
                for capability in schemas:
                    # Capabilities look like: urn:ietf:params:xml:ns:yang:ietf-interfaces?module=ietf-interfaces&revision=2014-05-08
                    if 'module=' in capability:
                        module = capability.split('module=')[1].split('&')[0]
                        module_list.append(module)
                
                self.logger.info(f"Found {len(module_list)} modules on device")
                
        except Exception as e:
            return (False, {
                "success": False,
                "error": f"Failed to connect to device: {str(e)}"
            })
        
        if not module_list:
            return (False, {
                "success": False,
                "error": "No YANG modules found on device"
            })
        
        # Download all modules concurrently
        connection_params = {
            'host': host,
            'port': port,
            'username': username,
            'password': password,
            'hostkey_verify': hostkey_verify,
            'device_params': {'name': 'default'}
        }
        
        self.logger.info(f"Downloading {len(module_list)} modules using {workers} workers...")
        results = self._download_concurrent(module_list, connection_params, 
                                           storage_path, workers, max_retries)
        
        # Count successes and failures
        downloaded = [r for r in results if r['success']]
        failed = [r for r in results if not r['success']]
        
        duration = time.time() - start_time
        
        self.logger.info(f"Complete! Downloaded: {len(downloaded)}, "
                        f"Failed: {len(failed)}, Duration: {duration:.1f}s")
        
        # Save index file
        index = {
            'host': host,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'total_modules': len(module_list),
            'downloaded': len(downloaded),
            'failed': len(failed),
            'duration_seconds': round(duration, 2),
            'failed_modules': [{'module': r['module'], 'error': r['error']} 
                              for r in failed]
        }
        
        with open(os.path.join(storage_path, 'index.json'), 'w') as f:
            json.dump(index, f, indent=2)
        
        return (True, {
            'success': True,
            'host': host,
            'storage_path': storage_path,
            'total_modules': len(module_list),
            'downloaded': len(downloaded),
            'failed': len(failed),
            'duration_seconds': round(duration, 2)
        })

    def _generate_pack_name(self, host):
        """
        Generate pack name from host
    
        Args:
            host: Device hostname/IP (e.g., '192.168.1.50')
    
        Returns:
            str: Pack name (e.g., 'device_192_168_1_50')
        """
        # Clean the host name: replace dots and dashes with underscores
        clean_name = host.replace('.', '_').replace('-', '_')
        return f"device_{clean_name}"
    
    def _download_concurrent(self, module_list, conn_params, storage_path, workers, max_retries):
        """Download modules using thread pool"""

        #Track downloaded results.
        results = []
        completed = 0
        total = len(module_list)
        
        # Create a pool of worker threads to download modules in parallel
        # Example: workers=10 means up to 10 modules downloading simultaneously
        with ThreadPoolExecutor(max_workers=workers) as executor:
            
            # Submit all download tasks to the thread pool
            # This creates a dictionary mapping each future (pending task) to its module name
            # All tasks start immediately (up to 'workers' limit)
            futures = {
                executor.submit(self._download_module, mod, conn_params, 
                              storage_path, max_retries): mod
                for mod in module_list
            }
            
            # Process results as downloads complete (not in submission order)
            # as_completed() yields futures as they finish, regardless of order
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                completed += 1
                
                # Log progress every 10 modules to avoid log spam
                if completed % 10 == 0:
                    success = sum(1 for r in results if r['success'])
                    self.logger.info(f"Progress: {completed}/{total} (Success: {success})")
        
        return results
    
    def _download_module(self, module_name, conn_params, storage_path, max_retries):
        """Download single module with retry logic"""
        
        #Track errors.
        retries = 0
        last_error = None
        
        while retries <= max_retries:
            try:
                with manager.connect(**conn_params) as m:
                    schema = m.get_schema(module_name)
                    
                    filepath = os.path.join(storage_path, f"{module_name}.yang")
                    with open(filepath, 'w') as f:
                        cleaned_content = self._clean_yang_content(schema.data)
                        f.write(cleaned_content)
                    
                    return {'module': module_name, 'success': True}
                    
            except Exception as e:
                last_error = str(e)
                retries += 1
                
                if retries <= max_retries:
                    time.sleep(2 ** (retries - 1))  # 1s, 2s, 4s
        
        return {'module': module_name, 'success': False, 'error': last_error}


    def _clean_yang_content(self, content):
        """
        Remove quotes from YANG keywords
    
        Some devices (Arista) return YANG with quoted keywords which pyang can't parse
        Example: module "name" → module name
        """
        import re
    
        patterns = [
            (r'module\s+"([^"]+)"', r'module \1'),
            (r'import\s+"([^"]+)"', r'import \1'),
            (r'prefix\s+"([^"]+)"', r'prefix \1'),
            (r'yang-version\s+"([^"]+)"', r'yang-version \1'),
            (r'namespace\s+"([^"]+)"', r'namespace "\1"'),  # Keep namespace quoted
        ]
    
        for pattern, replacement in patterns:
            content = re.sub(pattern, replacement, content)
    
        return content