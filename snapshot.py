from __future__ import annotations
import sys
from datetime import datetime, timedelta
import time
import logging
import threading
from configuration import LOG_LEVEL
from google.cloud import compute_v1
from google.api_core.exceptions import Conflict, RetryError, GoogleAPICallError

from common import fetch_instances, latest_snapshot, get_clients_and_projectID

logging.basicConfig(level=f"logging.{LOG_LEVEL}", format='%(asctime)s - %(levelname)s - %(message)s')

# Polls GCP API every five seconds to fetch snapshot status, breaks if snapshot
# marked as READY or FAILED which are the two valid completion statuses
def wait_for_snapshot_completion(project, snapshot_name, snapshot_client, disk_name):
      while True:
            operation = snapshot_client.get(project= project, snapshot= snapshot_name)
            status = operation.status
            
            if status == "READY":
                  logging.info(f"Snapshot for disk {disk_name} is Status.{status}")
                  break
            elif status == "FAILED":
                  logging.info(f"Snapshot for disk {disk_name} is Status.{status}")
                  break
            else:
                  logging.info(f"Snapshot for disk {disk_name} is Status.{status}")
                  time.sleep(5)

# Creates a new snapshot of named disk in named project
# Storage location and disk_project_id optional
# Error handling for Conflic error if snapshot name has been used before as they must have unique names
def create_snapshot(
        snapshot_client,
        project_id: str, 
        disk_name: str,
        snapshot_name: str,
        disk_url: str,
        zone: str, 
        location: str | None = None,
        disk_project_id: str | None = None,
) -> compute_v1.Snapshot:
        if zone is None:
                raise RuntimeError(
                        "Please specify 'zone'"
                )
    
        if disk_project_id is None:
                disk_project_id = project_id

        # Set snapshot configurations
        snapshot = compute_v1.Snapshot()
        snapshot.source_disk = disk_url
        snapshot.name = snapshot_name

        # If location set, use this, otherwise use GCP default storage location
        if location:
                snapshot.storage_locations = [location]

        try:
                # Create snapshot
                snapshot_client.insert(project=project_id, snapshot_resource=snapshot)
                # Poll for results of snapshot creation process
                wait_for_snapshot_completion(project_id, snapshot_name, snapshot_client, disk_name)
                # When marked as READY or FAILED, return snapshot
                return snapshot_client.get(project=project_id, snapshot=snapshot_name)

        except Conflict:
              logging.error(f'ERROR: Snapshot with name {snapshot_name} already exists')
              return None
        except (RetryError, GoogleAPICallError) as e:
              logging.error(f"Snapshot creation failed for {snapshot.name}: {e}")
              return None
              

# Creates new snapshot if previous backup was created at least one day ago
def snapshot(zone, snapshot_name):

        # Create InstancesClient and SnapshotsClient
        instances_client, snapshot_client, project_id = get_clients_and_projectID()
        logging.info("Starting backup process")
       
        today = datetime.today().strftime('%d-%m-%Y')  
 
         # Fetch intances with InstancesClient
        instances = fetch_instances(project_id, zone, instances_client)
        number_of_instances = 0
        for instance in instances:
               number_of_instances+= 1

        logging.info(f"Found {number_of_instances} instances")

        # Check if backups are enabled in disks of each instance
        for instance in instances:
                disks = instance.disks
                backupEnabled = instance.labels.get('backup')
                disk_name= instance.name
                # If enabled, start backup process
                if backupEnabled =="true":
                        logging.info(f"Instance: {instance.name}")
                        logging.info("Backup Enabled: True")
                        # Check when last snapshot produced, if less than one day ago, do not proceed with snapshot creation
                        # If more than one day ago, proceed with backup creation
                        for disk in disks:
                                disk_url = disk.source
                                disk_project_id = project_id
                                today = datetime.now().date()
                                now = datetime.now().astimezone(None)
                                snapshot_timestamp = latest_snapshot(snapshot_client, disk_url, project_id)
                                snapshot_date = snapshot_timestamp.date()
                                difference = today - snapshot_date
                                timeAgo = now - snapshot_timestamp
                                logging.info(f"Last backup was {timeAgo} ago")
                                if difference == timedelta(days=0):
                                        logging.info(f"Skipping backup creation since the latest backup is too recent") 

                                else:
                                        logging.info("Starting backup creation")
                                        thread = threading.Thread(target=create_snapshot, args=(snapshot_client, project_id, disk_name, snapshot_name, disk_url, zone, None, disk_project_id))
                                        thread.start()
                else:
                        logging.info(f"Instance: {instance.name}")
                        logging.info("Backup Enabled: False")
                                       
                                      
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Please check you have provided your zone and snapshot name")
        sys.exit(1)

    zone = sys.argv[1]
    snapshot_name = sys.argv[2]
    snapshot(zone, snapshot_name)    
