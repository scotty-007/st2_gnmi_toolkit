#!/usr/bin/env python3
"""
Pack Utilities - Shared functions for pack management
Used by downloader, parser, and generator to maintain consistent naming
"""
def generate_pack_name(device_identifier):
    """
    Generate device pack name from device identifier
    
    Ensures consistent pack naming across all actions:
    - Downloader creates: device_X/yang_models/
    - Parser reads from: device_X/yang_models/
    - Generator writes to: device_X/actions/
    
    Args:
        device_identifier: Device hostname or IP (e.g., '192.168.1.50', 'core-switch-1')
    
    Returns:
        str: Pack name (e.g., 'device_192_168_1_50', 'device_core_switch_1')
    
    Examples:
        '192.168.1.50' -> 'device_192_168_1_50'
        'core-switch-1' -> 'device_core_switch_1'
        'switch.example.com' -> 'device_switch_example_com'
    """
    # Replace dots and dashes with underscores for valid pack name
    clean_name = device_identifier.replace('.', '_').replace('-', '_')
    
    # Ensure it doesn't start with a number (StackStorm pack requirement)
    # Pack names must start with a letter
    return f"device_{clean_name}"
def get_pack_base_dir(device_identifier, base_path="/opt/stackstorm/packs.dev"):
    """
    Get full path to device pack directory
    
    Args:
        device_identifier: Device hostname or IP
        base_path: Base packs directory (default: /opt/stackstorm/packs.dev)
    
    Returns:
        str: Full path to pack directory
    
    Example:
        get_pack_base_dir('192.168.1.50')
        -> '/opt/stackstorm/packs.dev/device_192_168_1_50'
    """
    pack_name = generate_pack_name(device_identifier)
    return f"{base_path}/{pack_name}"
def get_yang_models_path(device_identifier, base_path="/opt/stackstorm/packs.dev"):
    """
    Get full path to YANG models directory for a device
    
    Args:
        device_identifier: Device hostname or IP
        base_path: Base packs directory
    
    Returns:
        str: Full path to YANG models raw directory
    
    Example:
        get_yang_models_path('192.168.1.50')
        -> '/opt/stackstorm/packs.dev/device_192_168_1_50/yang_models/raw'
    """
    pack_dir = get_pack_base_dir(device_identifier, base_path)
    return f"{pack_dir}/yang_models/raw"