#!/usr/bin/env python3

import requests
import subprocess
import time
import logging
import json
import argparse
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Disable SSL warnings (optional, not recommended for production)
requests.packages.urllib3.disable_warnings()

class ONTAPRestClient:
    def __init__(self, host, username, password, verify_ssl=False):
        self.base_url = f"https://{host}/api"
        self.auth = (username, password)
        self.verify_ssl = verify_ssl
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def _make_request(self, method, endpoint, data=None):
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.request(
                method,
                url,
                auth=self.auth,
                headers=self.headers,
                json=data,
                verify=self.verify_ssl
            )
            response.raise_for_status()
            return response.json() if response.content else None
        except requests.exceptions.RequestException as e:
            logger.error(f"REST request failed: {str(e)}")
            raise

def validate_source_volume(client, svm_name, volume_name):
    """Validate that the source volume exists"""
    print(f"Validating source volume: {svm_name}:{volume_name}")
    try:
        volumes = client._make_request(
            'GET',
            f"storage/volumes?name={volume_name}&svm.name={svm_name}&fields=name,svm.name"
        )
        if not volumes.get('records'):
            raise ValueError(f"Volume {volume_name} not found on SVM {svm_name}")
        print(f"Source volume {svm_name}:{volume_name} validated successfully")
        return True
    except Exception as e:
        print(f"Error validating source volume: {str(e)}")
        print("Please verify the SVM name and volume name are correct.")
        logger.error(f"Failed to validate source volume: {str(e)}")
        return False

def get_destination_path(client, source_path):
    """Get destination path using snapmirror/list-destinations"""
    print(f"Finding destination path for source: {source_path}")
    try:
        destinations = client._make_request(
            'GET',
            f"snapmirror/destinations?source.path={source_path}&fields=destination.path"
        )
        
        if not destinations.get('records'):
            raise ValueError(f"No SnapMirror destination found for source path: {source_path}")
        
        destination_path = destinations['records'][0]['destination']['path']
        print(f"Found destination path: {destination_path}")
        return destination_path
    except Exception as e:
        print(f"Error finding destination path: {str(e)}")
        print("Troubleshooting suggestions:")
        print("- Verify that a SnapMirror relationship exists for this source path")
        print("- Check the source path format (should be <svm_name>:<volume_name>)")
        print("- Ensure the source volume has a configured SnapMirror destination")
        print("- Confirm API access and permissions to the cluster")
        logger.error(f"Failed to find destination path: {str(e)}")
        return None

def update_snapmirror(client, source_path, destination_path):
    """Perform SnapMirror update"""
    print("Starting SnapMirror update...")
    try:
        print(f"Looking up SnapMirror relationship: {source_path} -> {destination_path}")
        relationships = client._make_request(
            'GET',
            f"snapmirror/relationships?source.path={source_path}&destination.path={destination_path}&fields=uuid"
        )
        if not relationships.get('records'):
            raise ValueError("SnapMirror relationship not found")
        uuid = relationships['records'][0]['uuid']
        print(f"Found relationship UUID: {uuid}")

        print("Initiating SnapMirror transfer...")
        client._make_request(
            'POST',
            f"snapmirror/relationships/{uuid}/transfers"
        )
        logger.info("SnapMirror update initiated")

        print("Waiting for SnapMirror transfer to complete...")
        while True:
            status = client._make_request(
                'GET',
                f"snapmirror/relationships/{uuid}?fields=state"
            )
            print(f"Current state: {status['state']}")
            if status['state'] != 'transferring':
                break
            time.sleep(5)

        print("SnapMirror update completed successfully")
        logger.info("SnapMirror update completed")
        return True
    except Exception as e:
        print(f"Error during SnapMirror update: {str(e)}")
        logger.error(f"SnapMirror update failed: {str(e)}")
        return False

def break_snapmirror(client, destination_path):
    """Break SnapMirror relationship"""
    print("Starting SnapMirror break operation...")
    try:
        print(f"Looking up SnapMirror relationship for destination: {destination_path}")
        relationships = client._make_request(
            'GET',
            f"snapmirror/relationships?destination.path={destination_path}&fields=uuid"
        )
        if not relationships.get('records'):
            raise ValueError("SnapMirror relationship not found")
        uuid = relationships['records'][0]['uuid']
        print(f"Found relationship UUID: {uuid}")

        print("Breaking SnapMirror relationship...")
        client._make_request(
            'PATCH',
            f"snapmirror/relationships/{uuid}",
            {"state": "broken_off"}
        )
        logger.info("SnapMirror relationship broken")
        print("SnapMirror relationship broken successfully")
        return True
    except Exception as e:
        print(f"Error breaking SnapMirror: {str(e)}")
        logger.error(f"Failed to break SnapMirror: {str(e)}")
        return False

def scan_iscsi():
    """Rescan iSCSI sessions on RHEL"""
    print("Starting iSCSI device rescan...")
    try:
        subprocess.run(['iscsiadm', '-m', 'node', '-R'], check=True)
        print("iSCSI rescan completed successfully")
        logger.info("iSCSI rescan completed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error during iSCSI rescan: {str(e)}")
        logger.error(f"iSCSI rescan failed: {str(e)}")
        return False

def refresh_multipath():
    """Refresh multipath devices on RHEL"""
    print("Starting multipath refresh...")
    try:
        subprocess.run(['multipath', '-r'], check=True)
        print("Multipath refresh completed successfully")
        logger.info("Multipath refresh completed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error during multipath refresh: {str(e)}")
        logger.error(f"Multipath refresh failed: {str(e)}")
        return False

def mount_volume(device_path, mount_point):
    """Mount the volume to specified mount point"""
    print(f"Starting mount operation for {device_path} to {mount_point}...")
    try:
        if os.path.exists(mount_point):
            print(f"Mount point {mount_point} already exists")
            if os.path.ismount(mount_point):
                print(f"Mount point {mount_point} is already mounted")
                return True
        else:
            print(f"Creating mount point directory: {mount_point}")
            subprocess.run(['mkdir', '-p', mount_point], check=True)

        print(f"Mounting device {device_path}...")
        subprocess.run(['mount', device_path, mount_point], check=True)
        print(f"Successfully mounted {device_path} to {mount_point}")
        logger.info(f"Successfully mounted {device_path} to {mount_point}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error during mount operation: {str(e)}")
        logger.error(f"Mount failed: {str(e)}")
        return False

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Perform SnapMirror operations and mount iSCSI LUN on RHEL'
    )
    parser.add_argument(
        '--host',
        required=True,
        help='ONTAP cluster management IP or hostname'
    )
    parser.add_argument(
        '--username',
        required=True,
        help='ONTAP admin username'
    )
    parser.add_argument(
        '--password',
        required=True,
        help='ONTAP admin password'
    )
    parser.add_argument(
        '--svm-name',
        required=True,
        help='SVM name for the source volume'
    )
    parser.add_argument(
        '--source-volume',
        required=True,
        help='Source volume name'
    )
    parser.add_argument(
        '--device-path',
        required=True,
        help='iSCSI device path (e.g., /dev/sdb)'
    )
    parser.add_argument(
        '--mount-point',
        required=True,
        help='Mount point path (e.g., /mnt/backup)'
    )
    parser.add_argument(
        '--verify-ssl',
        action='store_true',
        help='Enable SSL verification (default: False)'
    )
    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_arguments()

    # Construct source path from SVM name and source volume
    source_path = f"{args.svm_name}:{args.source_volume}"

    print("Initializing backup volume setup process...")
    print(f"Using source path: {source_path}")
    
    # Initialize REST client with command line parameters
    client = ONTAPRestClient(args.host, args.username, args.password, args.verify_ssl)
    print(f"Connected to ONTAP system at {args.host}")

    # Validate source volume exists
    if not validate_source_volume(client, args.svm_name, args.source_volume):
        return

    # Get destination path from source path
    destination_path = get_destination_path(client, source_path)
    if not destination_path:
        return

    # Perform SnapMirror update
    if not update_snapmirror(client, source_path, destination_path):
        return

    # Break SnapMirror relationship
    if not break_snapmirror(client, destination_path):
        return

    print("Waiting for system to recognize changes (10 seconds)...")
    time.sleep(10)

    # Scan iSCSI devices
    if not scan_iscsi():
        return

    # Refresh multipath devices
    if not refresh_multipath():
        return

    print("Waiting for device availability (5 seconds)...")
    time.sleep(5)

    # Mount the volume
    if not mount_volume(args.device_path, args.mount_point):
        return

    print("Backup volume setup completed successfully!")
    logger.info("Backup volume setup completed successfully")

if __name__ == "__main__":
    main()