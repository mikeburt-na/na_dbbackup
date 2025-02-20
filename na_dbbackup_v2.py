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
            error_detail = f"{e}"
            if hasattr(e, 'response') and e.response is not None:
                error_detail += f" - Response: {e.response.status_code} {e.response.text}"
            logger.error(f"REST request failed: {error_detail}")
            raise Exception(error_detail)

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
        print(f"Source volume validated successfully")
        return True
    except Exception as e:
        print(f"Error: Failed to validate source volume - {str(e)}")
        logger.error(f"Failed to validate source volume: {str(e)}")
        return False

def get_destination_path(client, source_path):
    """Get destination path using snapmirror/relationships with list_destinations_only"""
    print(f"Finding SnapMirror destination for source: {source_path}")
    try:
        relationships = client._make_request(
            'GET',
            f"snapmirror/relationships?source.path={source_path}&list_destinations_only=true&fields=destination.path"
        )
        if not relationships.get('records'):
            raise ValueError(f"No SnapMirror relationship found for source path: {source_path}")
        destination_path = relationships['records'][0]['destination']['path']
        print(f"Destination found: {destination_path}")
        return destination_path
    except Exception as e:
        print(f"Error: Failed to find destination path - {str(e)}")
        logger.error(f"Failed to find destination path: {str(e)}")
        return None

def update_snapmirror(client, source_path, destination_path):
    """Perform SnapMirror update and ensure it’s fully completed"""
    print(f"Starting SnapMirror update from {source_path} to {destination_path}")
    try:
        print("Retrieving relationship details...")
        relationships = client._make_request(
            'GET',
            f"snapmirror/relationships?source.path={source_path}&destination.path={destination_path}&fields=uuid,state,transfer.state"
        )
        if not relationships.get('records'):
            raise ValueError("SnapMirror relationship not found")
        uuid = relationships['records'][0]['uuid']
        print(f"Relationship UUID: {uuid}")

        print("Initiating SnapMirror transfer...")
        client._make_request(
            'POST',
            f"snapmirror/relationships/{uuid}/transfers"
        )
        logger.info("SnapMirror update initiated")

        print("Waiting for SnapMirror update to complete...")
        max_attempts = 24  # 120 seconds total
        attempt = 0
        while attempt < max_attempts:
            status = client._make_request(
                'GET',
                f"snapmirror/relationships/{uuid}?fields=state,transfer.state"
            )
            rel_state = status['state']
            transfer_state = status.get('transfer', {}).get('state', 'none')
            print(f"Current status - Relationship: {rel_state}, Transfer: {transfer_state}")
            if rel_state == 'snapmirrored' and transfer_state in ['none', 'success', 'failed']:
                break
            time.sleep(5)
            attempt += 1

        if attempt >= max_attempts:
            raise ValueError("SnapMirror update did not complete within 120 seconds")

        print("SnapMirror update completed successfully")
        logger.info("SnapMirror update completed")
        return True
    except Exception as e:
        print(f"Error: SnapMirror update failed - {str(e)}")
        logger.error(f"SnapMirror update failed: {str(e)}")
        return False

def quiesce_snapmirror(client, uuid):
    """Quiesce the SnapMirror relationship using 'paused' state"""
    print("Pausing SnapMirror relationship...")
    try:
        response = requests.patch(
            f"{client.base_url}/snapmirror/relationships/{uuid}",
            auth=client.auth,
            headers=client.headers,
            json={"state": "paused"},
            verify=client.verify_ssl
        )
        response.raise_for_status()
        print("Pause request sent successfully")
        logger.info("SnapMirror pause request sent")

        job_info = response.json() if response.content else {}
        job_id = job_info.get('job', {}).get('uuid')
        
        if job_id:
            print(f"Monitoring pause job: {job_id}")
            max_attempts = 24
            attempt = 0
            while attempt < max_attempts:
                job_status = client._make_request(
                    'GET',
                    f"cluster/jobs/{job_id}?fields=state"
                )
                job_state = job_status['state']
                print(f"Pause job status: {job_state}")
                if job_state in ['success', 'failure']:
                    break
                time.sleep(5)
                attempt += 1

            if job_state == 'failure':
                raise ValueError("Pause job failed")

        max_attempts = 24
        attempt = 0
        while attempt < max_attempts:
            status = client._make_request(
                'GET',
                f"snapmirror/relationships/{uuid}?fields=state"
            )
            current_state = status['state']
            print(f"Current state: {current_state}")
            if current_state == 'paused':
                print("SnapMirror relationship paused successfully")
                return True
            time.sleep(5)
            attempt += 1

        raise ValueError("Failed to pause SnapMirror within 120 seconds")
    except Exception as e:
        print(f"Error: Failed to pause SnapMirror - {str(e)}")
        logger.error(f"Failed to pause SnapMirror: {str(e)}")
        return False

def break_snapmirror(client, destination_path):
    """Break SnapMirror relationship after pausing"""
    print(f"Breaking SnapMirror relationship for destination: {destination_path}")
    try:
        print("Retrieving relationship details...")
        relationships = client._make_request(
            'GET',
            f"snapmirror/relationships?destination.path={destination_path}&fields=uuid,state,transfer.state"
        )
        if not relationships.get('records'):
            raise ValueError("SnapMirror relationship not found")
        uuid = relationships['records'][0]['uuid']
        current_state = relationships['records'][0]['state']
        transfer_state = relationships['records'][0].get('transfer', {}).get('state', 'none')
        print(f"Relationship UUID: {uuid}, Current state: {current_state}, Transfer state: {transfer_state}")

        if current_state != 'paused':
            if current_state != 'snapmirrored':
                raise ValueError(f"Cannot break SnapMirror: current state is '{current_state}', must be 'snapmirrored' or 'paused'")
            if not quiesce_snapmirror(client, uuid):
                return False

        print("Initiating SnapMirror break...")
        response = requests.patch(
            f"{client.base_url}/snapmirror/relationships/{uuid}",
            auth=client.auth,
            headers=client.headers,
            json={"state": "broken_off"},
            verify=client.verify_ssl
        )
        response.raise_for_status()
        print("Break request sent successfully")
        logger.info("SnapMirror break request sent")

        job_info = response.json() if response.content else {}
        job_id = job_info.get('job', {}).get('uuid')

        if job_id:
            print(f"Monitoring break job: {job_id}")
            max_attempts = 24
            attempt = 0
            while attempt < max_attempts:
                job_status = client._make_request(
                    'GET',
                    f"cluster/jobs/{job_id}?fields=state"
                )
                job_state = job_status['state']
                print(f"Break job status: {job_state}")
                if job_state in ['success', 'failure']:
                    break
                time.sleep(5)
                attempt += 1

            if job_state == 'failure':
                raise ValueError("Break job failed")

        print("Waiting for SnapMirror relationship to break...")
        max_attempts = 24
        attempt = 0
        while attempt < max_attempts:
            status = client._make_request(
                'GET',
                f"snapmirror/relationships/{uuid}?fields=state"
            )
            current_state = status['state']
            print(f"Current state: {current_state}")
            if current_state == 'broken_off':
                print("SnapMirror relationship broken successfully")
                logger.info("SnapMirror relationship broken")
                return True
            time.sleep(5)
            attempt += 1

        raise ValueError("Failed to break SnapMirror within 120 seconds")
    except Exception as e:
        print(f"Error: Failed to break SnapMirror - {str(e)}")
        logger.error(f"Failed to break SnapMirror: {str(e)}")
        return False

def scan_iscsi():
    """Rescan iSCSI sessions on RHEL"""
    print("Scanning iSCSI devices...")
    try:
        subprocess.run(['iscsiadm', '-m', 'node', '-R'], check=True)
        print("iSCSI scan completed successfully")
        logger.info("iSCSI rescan completed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error: iSCSI scan failed - {str(e)}")
        logger.error(f"iSCSI rescan failed: {str(e)}")
        return False

def refresh_multipath():
    """Refresh multipath devices on RHEL"""
    print("Refreshing multipath devices...")
    try:
        subprocess.run(['multipath', '-r'], check=True)
        print("Multipath refresh completed successfully")
        logger.info("Multipath refresh completed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error: Multipath refresh failed - {str(e)}")
        logger.error(f"Multipath refresh failed: {str(e)}")
        return False

def mount_volume(device_path, mount_point):
    """Mount the volume to specified mount point"""
    print(f"Mounting {device_path} to {mount_point}...")
    try:
        if os.path.exists(mount_point):
            print(f"Mount point {mount_point} already exists")
            if os.path.ismount(mount_point):
                print(f"{mount_point} is already mounted")
                return True
        else:
            print(f"Creating mount point: {mount_point}")
            subprocess.run(['mkdir', '-p', mount_point], check=True)

        subprocess.run(['mount', device_path, mount_point], check=True)
        print(f"Successfully mounted {device_path} to {mount_point}")
        logger.info(f"Successfully mounted {device_path} to {mount_point}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error: Mount operation failed - {str(e)}")
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

    print("Initializing SnapMirror backup process...")
    print(f"Source path: {source_path}")
    
    # Initialize REST client with command line parameters
    client = ONTAPRestClient(args.host, args.username, args.password, args.verify_ssl)
    print(f"Connected to ONTAP system: {args.host}")

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

    print("Waiting for system to process changes (10 seconds)...")
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

    print("SnapMirror backup process completed successfully!")
    logger.info("Backup volume setup completed successfully")

if __name__ == "__main__":
    main()