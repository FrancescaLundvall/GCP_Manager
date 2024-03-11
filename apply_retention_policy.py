from __future__ import annotations
import sys
from datetime import datetime, timedelta, timezone
import logging
from configuration import LOG_LEVEL, disk_id

from common import fetch_snapshots, delete_named_snapshot, get_clients_and_projectID

logging.basicConfig(level=f"logging.{LOG_LEVEL}", format='%(asctime)s - %(levelname)s - %(message)s')

# Applies following retention policy on snapshots for specified disk:
# No more than one backup per day should be kept for backups made in the last 7 days
# No more than one backup per week should be kept for backups made prior to the last 7 days
# When removing backups that don’t match the retention policy, keep the most recent backup that still fits.
def apply_retention_policy(zone):

    # Create InstancesClient and SnapshotsClient
    instances_client, snapshot_client, project_id = get_clients_and_projectID()
    snapshots_by_day = []
    snapshots_by_week = []
    to_delete = []

    # Get the current date
    current_date = datetime.now(timezone.utc)

    # Fetch snapshots with SnapshotsClient
    snapshots = fetch_snapshots(project_id, snapshot_client)

    snapshots_filtered = []

    logging.info("Checking backups against retention policy")
    # Filter out any snapshots with different disk id than specified disk
    for snapshot in snapshots:
         if snapshot.source_disk_id == disk_id:
            snapshots_filtered.append(snapshot)

    # Used to validate final sorting of snapshots 
    snapshots_length = 0

    # Sort filtered snapshots in based on creation_timestamp
    snapshots_sorted = sorted(snapshots_filtered, key = lambda x : x.creation_timestamp)

    logging.info(f"Checking backups against for disk {disk_id}")
    for snapshot in snapshots_sorted:
        snapshots_length += 1

        timestamp = datetime.fromisoformat(snapshot.creation_timestamp)
        
        # If the snapshot's timestamp is from the past 7 days, store weekday value
        if (current_date - timestamp) <= timedelta(days=7):
            iso_weekday = timestamp.isocalendar().weekday
            snapshots_by_day.append((snapshot, iso_weekday))

        # If the snapshot's timestamp is older than 7 days store calendar week value
        elif(current_date - timestamp) > timedelta(days=7):
            iso_week = timestamp.isocalendar().week
            snapshots_by_week.append((snapshot, iso_week))

    if len(snapshots_by_day) != 0:
        logging.info(f"Found {len(snapshots_by_day)} backups made in the past 7 days")


    # Creating a dict of the most recent snapshot for each day for the past seven days based on the weekday value
    # snapshots_by_day is necessarily sorted in order, and dicts do not allow key duplication, therefore the first of each weekday value processed must also be the most recent
    snapshots_to_keep_daily = {t[1]: t for t in snapshots_by_day}
    

    if len(snapshots_by_week) != 0:
         logging.info(f"Found {len(snapshots_by_week)} backups made more than 7 days ago")

    # Creating a dict of the most recent snapshot for each week for snapshots older than past seven days
    # snapshots_by_week is necessarily sorted in order, and dicts do not allow key duplication, therefore the first of each weekly value processed must also be the most recent
    snapshots_to_keep_weekly = {t[1]: t for t in snapshots_by_week}

    # Combining arrays for easier filtering
    to_keep = [t[0] for t in snapshots_to_keep_daily.values()] + [t[0] for t in snapshots_to_keep_weekly.values()]
    
    # For each snapshot that is in snapshots_sorted but NOT in to_keep, add it to to_delete
    # This is because all valid snapshots are in to_keep, therefore any invalid ones will not be in it
    to_delete = [snapshot for snapshot in snapshots_sorted if snapshot not in to_keep]
    
    # Final check that the total number of entries in to_keep and to_delete is equal to the total entries in snapshots_sorted before deleting all invalid snapshots
    if snapshots_length > (len(to_delete)+len(to_keep)):
            logging.error(f'ERROR: Total number of snapshots and filtered results totals do not match up, not all snapshots included in filtering')
            return None
    elif snapshots_length < (len(to_delete)+len(to_keep)):
            logging.error(f'ERROR: Total number of snapshots and filtered result totals do not match up, too many filtered results')
            return None
    else:
        for snapshot in to_delete:
            logging.info(f'Deleting snapshot {snapshot.name}')
            result = delete_named_snapshot(project_id,snapshot.name, snapshot_client)

    logging.info("Retention Policy applied")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Please check you have provided your zone")
        sys.exit(1)

    zone = sys.argv[1]
    apply_retention_policy(zone)    
