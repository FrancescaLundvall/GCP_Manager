## Scripts for automated management of GCP Instances

This Python application allows users to use the command line to display instance information, create disk snapshots, and apply a preprogrammed retention policy 

### Requirements
- Python 3.12.2
- GCP credentials json for accessing GCP instance

### To run project
- Clone repository
- Create virtual environment in project root
- Activate virtual environment 
- Install requirements from requirements.txt with `pip install -r requirements.txt`
- Add your credentials json to a folder in project route 
- Update `path_to_credentials` and `disk_id` in configuration.py

### Scripts
There are three scripts in this project to handle GCP backup management
- `backup_table.py`  takes the argument `zone` and produces a table with details of instances, whether backups are enabled on their disks, and when the latest backup was created
- `snapshot.py` takes two arguments: `zone` and `snapshot_name`. This is because snapshot names must be unique for each snapshot created. This script will check that no other snapshots were produced that day and if not, create a new backup
- `apply_retention_policy.py` takes the argument `zone` and applies the given retention policy to the disk with disk_id set in configuration.py.

### Run commands
python backup_table.py $zone

python snapshot.py $zone $snapshot_name

python retention_policy.py $zone
