from __future__ import print_function

import boto3
from datetime import datetime
import sys
import logging
import json
import pytz


class BaseBackupManager(object):

    def __init__(self, period, tag_name, tag_value, date_suffix, keep_count):

        # Message to return result
        self.message = ""
        self.errmsg = ""

        self.period = period
        self.tag_name = tag_name
        self.tag_value = tag_value
        self.date_suffix = date_suffix
        self.keep_count = keep_count

    def lookup_period_prefix(self):
        return self.period

    def get_resource_tags(self, resource_id):
        pass

    def set_resource_tags(self, resource, tags):
        pass

    def get_backable_resources(self):
        pass

    def snapshot_resource(self, resource, description, tags):
        pass

    def list_snapshots_for_resource(self, resource):
        pass

    def resolve_backupable_id(self, resource):
        pass

    def resolve_snapshot_time(self, resource):
        return resource['StartTime']

    def process_backup(self):
        # Setup logging
        start_message = 'Started taking %(period)s snapshots at %(date)s' % {
            'period': self.period,
            'date': datetime.today().strftime('%d-%m-%Y %H:%M:%S')
        }
        self.message = start_message + "\n\n"
        print(start_message)

        # Counters
        total_creates = 0
        total_deletes = 0
        count_errors = 0

        # Number of snapshots to keep
        count_success = 0
        count_total = 0

        backupables = self.get_backable_resources()
        for backup_item in backupables:

            count_total += 1
            backup_id = self.resolve_backupable_id(backup_item)

            self.message += 'Processing backup item %(id)s\n' % {
                'id': backup_id
            }

            try:
                tags_volume = self.get_resource_tags(backup_id)
                description = '%(period)s_snapshot %(item_id)s_%(period)s_%(date_suffix)s by snapshot script at %(date)s' % {
                    'period': self.period,
                    'item_id': backup_id,
                    'date_suffix': self.date_suffix,
                    'date': datetime.today().strftime('%d-%m-%Y %H:%M:%S')
                }
                try:
                    self.snapshot_resource(resource=backup_item, description=description, tags=tags_volume)
                    self.message += '    New Snapshot created with description: %s and tags: %s\n' % (description, str(tags_volume))
                    total_creates += 1
                except Exception, e:
                    print ("Unexpected error:", sys.exc_info()[0])
                    print (e)
                    pass

                snapshots = self.list_snapshots_for_resource(resource=backup_item)
                deletelist = []

                # Sort the list based on the dates of the objects
                snapshots.sort(self.date_compare)

                for snap in snapshots:
                    sndesc = self.resolve_snapshot_name(snap)
                    if sndesc.startswith(self.lookup_period_prefix()):
                        deletelist.append(snap)
                    else:
                        print('  Skipping other backup schedule: ' + sndesc)

                self.message += "\n    Current backups in rotation (keeping {})\n".format(self.keep_count)
                self.message += "    ---------------------------\n"

                for snap in deletelist:
                    self.message += "    {} - {}\n".format(self.resolve_snapshot_name(snap),
                                                           self.resolve_snapshot_time(snap))
                self.message += "    ---------------------------\n"

                deletelist.sort(self.date_compare)
                delta = len(deletelist) - self.keep_count

                for i in range(delta):
                    self.message += '    Deleting snapshot ' + self.resolve_snapshot_name(deletelist[i]) + '\n'
                    self.delete_snapshot(deletelist[i])
                    total_deletes += 1
                # time.sleep(3)
            except Exception as ex:
                print("Unexpected error:", sys.exc_info()[0])
                print(ex)
                logging.error('Error in processing volume with id: ' + backup_id)
                self.errmsg += 'Error in processing volume with id: ' + backup_id
                count_errors += 1
            else:
                count_success += 1

        result = '\nFinished making snapshots at %(date)s with %(count_success)s snapshots of %(count_total)s possible.\n\n' % {
            'date': datetime.today().strftime('%d-%m-%Y %H:%M:%S'),
            'count_success': count_success,
            'count_total': count_total
        }

        self.message += result
        self.message += "\nTotal snapshots created: " + str(total_creates)
        self.message += "\nTotal snapshots errors: " + str(count_errors)
        self.message += "\nTotal snapshots deleted: " + str(total_deletes) + "\n"

    def delete_snapshot(self, snapshot):
        pass


class EC2BackupManager(BaseBackupManager):

    def __init__(self, ec2_region_name, period, tag_name, tag_value, date_suffix, keep_count):
        super(EC2BackupManager, self).__init__(period=period,
                                               tag_name=tag_name,
                                               tag_value=tag_value,
                                               date_suffix=date_suffix,
                                               keep_count=keep_count)

        # Connect to AWS using the credentials provided above or in Environment vars or using IAM role.
        print('Connecting to AWS')
        self.conn = boto3.client('ec2', region_name=ec2_region_name)

    @staticmethod
    def date_compare(snap1, snap2):
        if snap1['StartTime'] < snap2['StartTime']:
            return -1
        elif snap1['StartTime'] == snap2['StartTime']:
            return 0
        return 1

    def lookup_period_prefix(self):
        return self.period + "_snapshot"

    def get_resource_tags(self, resource_id):
        resource_tags = {}
        if resource_id:
            tags = self.conn.describe_tags(Filters=[{"Name": "resource-id",
                                                    "Values": [resource_id]}])
            for tag in tags["Tags"]:
                # Tags starting with 'aws:' are reserved for internal use
                if not tag['Key'].startswith('aws:'):
                    resource_tags[tag['Key']] = tag['Value']
        return resource_tags

    def set_resource_tags(self, resource, tags):
        resource_id = resource['SnapshotId']
        for tag_key, tag_value in tags.iteritems():
            print('Tagging %(resource_id)s with [%(tag_key)s: %(tag_value)s]' % {
                    'resource_id': resource_id,
                    'tag_key': tag_key,
                    'tag_value': tag_value
                  })

            self.conn.create_tags(Resources=[resource_id],
                                  Tags=[{"Key": tag_key, "Value": tag_value}])

    def get_backable_resources(self):
        # Get all the volumes that match the tag criteria
        print('Finding volumes that match the requested tag ({ "tag:%(tag_name)s": "%(tag_value)s" })' % {
                                                                'tag_name': self.tag_name,
                                                                'tag_value': self.tag_value
                                                            })
        volumes = self.conn.describe_volumes(Filters=[{"Name": 'tag:' + self.tag_name,
                                                       "Values": [self.tag_value]}])["Volumes"]

        print('Found %(count)s volumes to manage' % { 'count': len(volumes) })

        return volumes

    def snapshot_resource(self, resource, description, tags):
        current_snap = self.conn.create_snapshot(VolumeId=self.resolve_backupable_id(resource),
                                                 Description=description)
        self.set_resource_tags(current_snap, tags)

    def list_snapshots_for_resource(self, resource):
        snapshots = self.conn.describe_snapshots(Filters=[
                                                 {"Name": "volume-id",
                                                  "Values": [self.resolve_backupable_id(resource)]
                                                  }])

        return snapshots['Snapshots']

    def resolve_backupable_id(self, resource):
        return resource["VolumeId"]

    def resolve_snapshot_name(self, resource):
        return resource['Description']

    def resolve_snapshot_time(self, resource):
        return resource['StartTime']

    def delete_snapshot(self, snapshot):
        self.conn.delete_snapshot(SnapshotId=snapshot["SnapshotId"])


class RDSBackupManager(BaseBackupManager):

    account_number = None

    def __init__(self, rds_region_name, period, tag_name, tag_value, date_suffix, keep_count):
        super(RDSBackupManager, self).__init__(period=period,
                                               tag_name=tag_name,
                                               tag_value=tag_value,
                                               date_suffix=date_suffix,
                                               keep_count=keep_count)

        # Connect to AWS using the credentials provided above or in Environment vars or using IAM role.
        print('Connecting to AWS')
        self.conn = boto3.client('rds', region_name=rds_region_name)

    @staticmethod
    def date_compare(snap1, snap2):
        utc = pytz.UTC
        now = datetime.utcnow().replace(tzinfo=utc)
        if snap1.get('SnapshotCreateTime', now) < snap2.get('SnapshotCreateTime', now):
            return -1
        elif snap1.get('SnapshotCreateTime', now) == snap2.get('SnapshotCreateTime', now):
            return 0
        return 1

    def lookup_period_prefix(self):
        return self.period

    def get_resource_tags(self, resource_id):
        resource_tags = {}
        if resource_id:
            arn = self.build_arn_for_id(resource_id)
            tags = self.conn.list_tags_for_resource(ResourceName=arn)['TagList']

            for tag in tags:
                # Tags starting with 'aws:' are reserved for internal use
                if not tag['Key'].startswith('aws:'):
                    resource_tags[tag['Key']] = tag['Value']
        return resource_tags

    def set_resource_tags(self, resource, tags):
        resource_id = resource['SnapshotId']
        for tag_key, tag_value in tags.iteritems():
            print('Tagging %(resource_id)s with [%(tag_key)s: %(tag_value)s]' % {
                    'resource_id': resource_id,
                    'tag_key': tag_key,
                    'tag_value': tag_value
                  })

            self.conn.create_tags(Resources=[resource_id],
                                  Tags=[{"Key": tag_key, "Value": tag_value}])

    def get_backable_resources(self):
        # Get all the volumes that match the tag criteria
        print('Finding databases that match the requested tag ({ "tag:%(tag_name)s": "%(tag_value)s" })' % {
                                                                 'tag_name': self.tag_name,
                                                                 'tag_value': self.tag_value
                                                             })
        all_instances = self.conn.describe_db_instances()['DBInstances']
        found = []

        for db_instance in all_instances:
            if self.db_has_tag(db_instance):
                found.append(db_instance)

        print('Found %(count)s databases to manage' % { 'count': len(found) })

        return found

    def snapshot_resource(self, resource, description, tags):

        aws_tagset = []
        for k in tags:
            aws_tagset.append({"Key": k, "Value": tags[k]})

        date = datetime.today().strftime('%d-%m-%Y-%H-%M-%S')
        snapshot_id = self.period+'-'+self.resolve_backupable_id(resource)+"-"+date+"-"+self.date_suffix

        current_snap = self.conn.create_db_snapshot(DBInstanceIdentifier=self.resolve_backupable_id(resource),
                                                    DBSnapshotIdentifier=snapshot_id,
                                                    Tags=aws_tagset)

    def list_snapshots_for_resource(self, resource):
        snapshots = self.conn.describe_db_snapshots(DBInstanceIdentifier=self.resolve_backupable_id(resource),
                                                    SnapshotType='manual')
        return snapshots['DBSnapshots']

    def resolve_backupable_id(self, resource):
        return resource["DBInstanceIdentifier"]

    def resolve_snapshot_name(self, resource):
        return resource['DBSnapshotIdentifier']

    def resolve_snapshot_time(self, resource):
        now = datetime.utcnow()
        return resource.get('SnapshotCreateTime', now)

    def delete_snapshot(self, snapshot):
        self.conn.delete_db_snapshot(DBSnapshotIdentifier=snapshot["DBSnapshotIdentifier"])

    def db_has_tag(self, db_instance):
        arn = self.build_arn(db_instance)
        tags = self.conn.list_tags_for_resource(ResourceName=arn)['TagList']

        for tag in tags:
            if tag['Key'] == self.tag_name and tag['Value'] == self.tag_value:
                return True

        return False

    def resolve_account_number(self):

        if self.account_number is None:
            groups = self.conn.describe_db_security_groups()['DBSecurityGroups']
            if groups is None or len(groups) == 0:
                self.account_number = 0
            else:
                self.account_number = groups[0]['OwnerId']

        return self.account_number

    def build_arn(self, instance):
        return self.build_arn_for_id(instance['DBInstanceIdentifier'])

    def build_arn_for_id(self, instance_id):
        # "arn:aws:rds:<region>:<account number>:<resourcetype>:<name>"

        region = self.conn.meta.region_name
        account_number = self.resolve_account_number()

        return "arn:aws:rds:{}:{}:db:{}".format(region, account_number, instance_id)


def lambda_handler(event, context={}):
    """
    Example content
        {
            "period_label": "day",
            "period_format": "%a%H",

            "ec2_region_name": "ap-southeast-2",
            "rds_region_name": "ap-southeast-2",

            "tag_name": "MakeSnapshot",
            "tag_value": "True",

            "arn": "blart",

            "keep_count": 12
        }
    :param event:
    :param context:
    :return:
    """

    print("Received event: " + json.dumps(event, indent=2))

    period = event["period_label"]
    period_format = event["period_format"]

    tag_name = event['tag_name']
    tag_value = event['tag_value']

    ec2_region_name = event.get('ec2_region_name', None)
    rds_region_name = event.get('rds_region_name', None)

    sns_arn = event.get('arn')
    error_sns_arn = event.get('error_arn')
    keep_count = event['keep_count']

    date_suffix = datetime.today().strftime(period_format)

    sns_boto = None

    # Connect to SNS
    if sns_arn or error_sns_arn:
        print('Connecting to SNS')
        sns_boto = boto3.client('sns', region_name=ec2_region_name)

    result = event
    if ec2_region_name:
        backup_mgr = EC2BackupManager(ec2_region_name=ec2_region_name,
                                      period=period,
                                      tag_name=tag_name,
                                      tag_value=tag_value,
                                      date_suffix=date_suffix,
                                      keep_count=keep_count)

        backup_mgr.process_backup()

        result["ec2_backup_result"] = backup_mgr.message
        print('\n' + backup_mgr.message + '\n')

        if error_sns_arn and backup_mgr.errmsg:
            sns_boto.publish(error_sns_arn, 'Error in processing volumes: ' + backup_mgr.errmsg, 'Error with AWS Snapshot')

        if sns_arn:
            sns_boto.publish(sns_arn, backup_mgr.message, 'Finished AWS EC2 snapshotting')

    if rds_region_name:
        backup_mgr = RDSBackupManager(rds_region_name=rds_region_name,
                                      period=period,
                                      tag_name=tag_name,
                                      tag_value=tag_value,
                                      date_suffix=date_suffix,
                                      keep_count=keep_count)

        backup_mgr.process_backup()

        result["rds_backup_result"] = backup_mgr.message
        print('\n' + backup_mgr.message + '\n')

        if error_sns_arn and backup_mgr.errmsg:
            sns_boto.publish(error_sns_arn, 'Error in processing RDS: ' + backup_mgr.errmsg, 'Error with AWS Snapshot')

        if sns_arn:
            sns_boto.publish(sns_arn, backup_mgr.message, 'Finished AWS RDS snapshotting')

    return json.dumps(result, indent=2)
