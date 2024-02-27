from google.oauth2 import service_account
import sys
from tabulate import tabulate
from configuration import LOG_LEVEL
import logging

from common import fetch_instances, latest_snapshot, get_clients_and_projectID

logging.basicConfig(level=f"logging.{LOG_LEVEL}", format='%(asctime)s - %(levelname)s - %(message)s')
   
# This function gets a table of each instance in selected project, and lists Instance name, 
# whether backups are enabled, what the disk name is, and when the last backup was 
def getTable(zone):
# Create InstancesClient and SnapshotsClient
    instances_client, snapshots_client, projectId = get_clients_and_projectID()

# Check zone is provided
    if zone is None:
        raise RuntimeError(
            "Please specify 'zone'"
        )
    
# Fetch intances with InstancesClient
    instances = fetch_instances(projectId, zone, instances_client)
    table = []

#  For each instance fetched that contains a disk:
#     Get timestamp of latest snapshot, the device name, and whether the label "backup" is set to True or False

    for instance in instances:
        disks = instance.disks
        instanceName = instance.name
        backupEnabled = instance.labels.get('backup')
        if len(disks) != 0:
            for disk in disks:
                disk_url = disk.source
                deviceName = disk.device_name
                latestSnapshot = latest_snapshot(snapshots_client, disk_url, projectId)
        else:
            latestSnapshot = "N/A"
            deviceName = "No Disks available for this instance"

        # Add instance information to a new table row and add row to table
        row = [instanceName, backupEnabled, deviceName, latestSnapshot]
        table.append(row) 

    # Format and output completed table 
    finalTable = tabulate(table, headers=['Instance', 'Backup Enabled', 'Disk', 'Latest Backup'], tablefmt='orgtbl')
    print(finalTable)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Please check you have provided your zone")
        sys.exit(1)

    zone = sys.argv[1]
    getTable(zone)    
