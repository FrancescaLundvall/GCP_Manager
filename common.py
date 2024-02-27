from google.oauth2 import service_account
from google.cloud import compute_v1
from datetime import datetime
from configuration import path_to_credentials, LOG_LEVEL
from google.api_core.exceptions import NotFound

import logging

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

# Library for project


# Function to set up credentials and InstancesClient and SnapshotsClient at the start of each script
def get_clients_and_projectID():  
    credentials = service_account.Credentials.from_service_account_file(path_to_credentials)

    instances_client = compute_v1.InstancesClient(credentials=credentials)
    snapshots_client = compute_v1.SnapshotsClient(credentials=credentials)

    projectID = credentials.project_id
    return instances_client, snapshots_client, projectID

# Fetch all instances in project and zone
def fetch_instances(project_id, zone, instances_client):
    request = compute_v1.ListInstancesRequest(
        project = project_id,
        zone = zone,
    )
    instances = instances_client.list(request = request)
    return instances

# Fetch all snapshots in project
def fetch_snapshots(project, snapshots_client):
    request = compute_v1.ListSnapshotsRequest(
        project = project,
    )
    snapshots = snapshots_client.list(request=request)
    return snapshots

# Returns the timestamp of the latest snapshot in ISO format
def latest_snapshot(snapshots_client, diskURL, projectId):
    snapshots = fetch_snapshots(projectId, snapshots_client)
    latest = None
    for snapshot in snapshots:
        if snapshot.source_disk == diskURL:
            snapshot_timestamp = datetime.fromisoformat(snapshot.creation_timestamp)
            if latest is None or snapshot_timestamp > latest:
            # snapshot_timestamps.append(snapshot_timestamp)
                latest = snapshot_timestamp

    if latest != None:
        return latest
    else:
        return "Never"
    
    # def latest_snapshot(snapshots_client, diskURL, projectId):
    # snapshots = fetch_snapshots(projectId, snapshots_client)
    # lastest = None
    # snapshot_timestamps = []
    # for snapshot in snapshots:
    #     if snapshot.source_disk == diskURL:
    #         snapshot_timestamp = datetime.fromisoformat(snapshot.creation_timestamp)
    #         if latest is None or snapshot_timestamp > latest:
    #         # snapshot_timestamps.append(snapshot_timestamp)
    #             latest = 

    # snapshot_timestamps.sort(reverse =True)

    # if len(snapshot_timestamps) != 0:
    #     latest = snapshot_timestamps[0]
    #     return latest
    # else:
    #     return "Never"

# Deletes named snapshot with error handling in case name of previously deleted snapshot is passed
def delete_named_snapshot(project_id, snapshot_name, snapshot_client):  
    request = compute_v1.DeleteSnapshotRequest(
    project=project_id,
    snapshot=snapshot_name,
    )
    try:
        response = snapshot_client.delete(request)
        return response
    except NotFound:
        logging.error(f'ERROR: Snapshot with name {snapshot_name} not found')
    return None
