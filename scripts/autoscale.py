#!/usr/bin/env python
"""
1. Get Data From Airflow API

2. Get hosts by autoscaling groups:
	Group by ip address

3. Set health of workers
	If in celery:
		healthy
	else:
		if not new:
			unhealthy

4. check if any workers not serving queues and no active_tasks:
	terminate

5. For each autoscaling group:
	if total > desired:
		for _ in range(total-desired):
			cancel consumer on oldest worker

6. Report backlog and capacity metrics by queue/autoscaling group

"""

import argparse
import collections
import datetime
import json
import logging
import urllib.parse

import backoff
import boto3
import requests


logging.getLogger('backoff').addHandler(logging.StreamHandler())


INITIALIZATION_SECONDS = 300
METRIC_NAMESPACE = 'EC2/Autoscaling'
OVER_CAPACITY = 1
ACCEPTABLE_CAPACITY = 0
UNDER_CAPACITY = -1


def get_stats_from_airflow(host):
    url = '{}/queue/stats'.format(host)
    response = requests.get(url)
    response.raise_for_status()
    return response.json()['data']


def get_autoscaling_group_data(autoscaling_group_names):
    autoscaling = boto3.client('autoscaling')
    autoscaling_groups = autoscaling.describe_auto_scaling_groups(
        AutoScalingGroupNames=autoscaling_group_names
    )["AutoScalingGroups"]

    return autoscaling_groups


def get_instances_ids_from_autoscaling_group(autoscaling_group):
    return [
        instance['InstanceId']
        for instance in autoscaling_group['Instances']
    ]


def get_instances(instance_ids):
    if not instance_ids:
        return []

    ec2 = boto3.client('ec2')
    payload = ec2.describe_instances(InstanceIds=instance_ids)
    instances = []
    for reservation in payload['Reservations']:
        for instance in reservation['Instances']:
            if instance['State']['Name'] == 'running':
                instances.append(instance)

    return instances


def mark_instance_healthy(instance_id):
    print('Marking instance <{}> as Healthy'.format(instance_id))
    # Set ec2 instance health


def mark_instance_unhealthy(instance_id):
    print('Marking instance <{}> as Unhealthy'.format(instance_id))
    # Set ec2 instance health


def terminate_instance(instance_id):
    print('Terminating instance <{}>'.format(instance_id))
    # boto3 terminate
    ec2 = boto3.client('ec2')
    return ec2.terminate_instances(
        InstanceIds=[
            instance_id
        ],
    )


def retire_worker(flower_host, worker, queue):
    print('Retiring Worker {!r}'.format(worker))
    # Refreshing to ensure the cache is fresh
    refresh_url = '{}/api/workers?refresh=True'.format(flower_host)
    print(requests.get(refresh_url).json().keys())
    url = '{}/api/worker/queue/cancel-consumer/{}?queue={}'.format(
        flower_host,
        worker,
        queue
    )
    return requests.post(url).json()


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--airflow', required=True)
    parser.add_argument('--flower')
    parser.add_argument('--config-path', required=True)

    return parser

def load_config(config_path):
    if config_path.startswith('s3://'):
        content = download_from_s3(config_path)
    else:
        raise RuntimeError

    config = json.loads(content)

    return config


@backoff.on_exception(
    backoff.expo,
    Exception,
    max_time=30,
)
def download_from_s3(path):
    location = urllib.parse.urlparse(path)
    s3 = boto3.client('s3')
    s3_data = s3.get_object(
        Bucket=location.netloc,
        Key=location.path.lstrip('/'),
    )
    content = s3_data['Body'].read()
    return content


def _main():
    parser = get_parser()
    options = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    host = options.airflow

    if options.flower:
        flower = options.flower
    else:
        flower = '{}:5555'.format(host)

    config = load_config(options.config_path)
    queue_map = config.get('queues', {})

    instances_by_ip = {}
    instances_by_worker = {}
    instances_by_id = {}
    worker_by_instance_id = {}
    worker_id_by_instance_id = {}
    terminated_instances = set()
    autoscaling_group_instance_id_map = {}
    autoscaling_group_map = {}
    autoscaling_groups = get_autoscaling_group_data(list(queue_map.values()))

    autoscaling_group_map.update({
        autoscaling_group['AutoScalingGroupName']: autoscaling_group
        for autoscaling_group in autoscaling_groups
    })

    for autoscaling_group in autoscaling_groups:
        instance_ids = get_instances_ids_from_autoscaling_group(autoscaling_group)
        autoscaling_group_instance_id_map[autoscaling_group['AutoScalingGroupName']] = set(instance_ids)
        instances = get_instances(instance_ids)
        instances_by_ip.update({
            instance['PrivateIpAddress']: instance
            for instance in instances
        })

        instances_by_id.update({
            instance['InstanceId']: instance
            for instance in instances
        })

    stats = get_stats_from_airflow(host)
    workers = stats['workers']

    instances_by_worker.update({
        worker_id: instances_by_ip[worker_id.split('@')[-1]]
        for worker_id in workers
        if worker_id.split('@')[-1] in instances_by_ip
    })

    worker_id_by_instance_id.update({
        instances_by_worker[worker_id]['InstanceId']: worker_id
        for worker_id in workers
        if worker_id in instances_by_worker
    })

    worker_by_instance_id.update({
        instances_by_worker[worker_id]['InstanceId']: worker
        for worker_id, worker in workers.items()
        if worker_id in instances_by_worker
    })

    for instance_id in instances_by_id:
        if instance_id in worker_by_instance_id:
            mark_instance_healthy(instance_id)
        else:
            instance = instances_by_id[instance_id]
            if (datetime.datetime.now(datetime.timezone.utc) - instance['LaunchTime']).seconds >  INITIALIZATION_SECONDS:
                mark_instance_unhealthy(instance_id)
        if instance_id in worker_by_instance_id:
            worker = worker_by_instance_id[instance_id]
            if not worker['queues'] and worker['active_tasks'] == 0:
                terminate_instance(instance_id)
                terminated_instances.add(instance_id)

    for autoscaling_group in autoscaling_groups:
        desired_capacity = autoscaling_group['DesiredCapacity']
        instances = sorted([
            instance for instance_id, instance in instances_by_id.items()
            if instance_id in worker_by_instance_id
            and worker_by_instance_id[instance_id]['queues']
            and instance_id not in terminated_instances
            and instance_id in autoscaling_group_instance_id_map[autoscaling_group['AutoScalingGroupName']]
        ], key=lambda x: x['LaunchTime'], reverse=True)

        if len(instances) > desired_capacity:
            instances_to_retire = [
                instance['InstanceId']
                for instance in instances[desired_capacity:]
            ]
            for instance_id in instances_to_retire:
                worker_id = worker_id_by_instance_id[instance_id]
                worker_info = workers[worker_id]
                queues = worker_info['queues']
                for queue in queues:
                    retire_worker(
                        flower,
                        worker_id,
                        queue
                    )

    report_metrics(host, queue_map, autoscaling_group_map)


@backoff.on_exception(
    backoff.expo,
    Exception,
    max_time=300,
)
def main():
    return _main()


def report_metrics(host, queue_map, autoscaling_group_map):
    stats = get_stats_from_airflow(host)
    now = datetime.datetime.utcnow()
    queues = stats['queues']
    workers_by_queue = collections.defaultdict(list)
    for worker_name, worker in stats['workers'].items():
        for queue in worker['queues']:
            workers_by_queue[queue].append(worker)

    capacity_metric_by_queue = calculate_capacity_metric(
        queues=queues,
        queue_map=queue_map,
        workers_by_queue=workers_by_queue,
    )

    backlog_metric_by_queue = calculate_backlog_metric(
        queues,
        queue_map
    )

    for queue_name, autoscaling_group_name in queue_map.items():
        autoscaling_group_name = queue_map.get(queue_name)
        autoscaling_group = autoscaling_group_map[autoscaling_group_name]

        capacity_metric = capacity_metric_by_queue[queue_name]
        backlog_metric = backlog_metric_by_queue[queue_name]

        if (
                (
                    autoscaling_group['DesiredCapacity'] == autoscaling_group['MinSize']
                    and capacity_metric == -1
                )
                or (
                    autoscaling_group['DesiredCapacity'] == autoscaling_group['MaxSize']
                    and capacity_metric == 1
                )
        ):
            capacity_metric = 0

        logging.info('Putting Metric Data: {}={} for {}'.format('capacity', capacity_metric, autoscaling_group_name))
        report_metric(
            autoscaling_group_name=autoscaling_group_name,
            metric='capacity',
            value=capacity_metric,
            timestamp=now
        )

        logging.info('Putting Metric Data: {}={} for {}'.format('backlog', backlog_metric, autoscaling_group_name))
        report_metric(
            autoscaling_group_name=autoscaling_group_name,
            metric='backlog',
            value=backlog_metric,
            timestamp=now
        )


def report_metric(autoscaling_group_name, metric, value, timestamp):
    cloudwatch = boto3.client('cloudwatch')

    cloudwatch.put_metric_data(
        Namespace=METRIC_NAMESPACE,
        MetricData=[
            {
                "MetricName": metric,
                "Dimensions": [
                    {
                        "Name": "AutoScalingGroupName",
                        "Value": autoscaling_group_name,
                    }
                ],
                "Value": value,
                "Timestamp": timestamp,
            },
        ]
    )


def calculate_capacity_metric(queues, workers_by_queue, queue_map):
    metric_by_queue = {}
    for queue_name, queue_info in queues.items():
        desired_usage = queue_info['desired_usage']
        max_capacity = queue_info['max_capacity']
        queue_backlog = queue_info['backlog']
        workers = len(workers_by_queue.get(queue_name, []))

        if max_capacity == 0:
            # CAPACITY
            if desired_usage > 0:
                metric_by_queue[queue_name] = OVER_CAPACITY
            elif desired_usage == 0:
                metric_by_queue[queue_name] = ACCEPTABLE_CAPACITY

        elif max_capacity > 0:

            # CAPACITY
            if workers > 1:
                if desired_usage == 0:
                    metric_by_queue[queue_name] = UNDER_CAPACITY
                elif desired_usage > max_capacity:
                    metric_by_queue[queue_name] = OVER_CAPACITY
                else:
                    metric_by_queue[queue_name] = ACCEPTABLE_CAPACITY

            else:
                if desired_usage > 0 and queue_backlog == 0:
                    metric_by_queue[queue_name] = ACCEPTABLE_CAPACITY
                elif desired_usage > max_capacity:
                    metric_by_queue[queue_name] = OVER_CAPACITY
                elif desired_usage == 0:
                    metric_by_queue[queue_name] = UNDER_CAPACITY
                else:
                    metric_by_queue[queue_name] = ACCEPTABLE_CAPACITY

    for queue_name in queue_map:
        if queue_name not in metric_by_queue:
            metric_by_queue[queue_name] = 0

    return metric_by_queue


def calculate_backlog_metric(queues, queue_map):
    metric_by_queue = {}

    for queue_name, queue_info in queues.items():
        max_capacity = queue_info['max_capacity']
        queue_backlog = queue_info['backlog']
        if max_capacity == 0:
            metric_by_queue[queue_name] = 0
        else:
            metric_by_queue[queue_name] = queue_backlog / max_capacity

    for queue_name in queue_map:
        if queue_name not in metric_by_queue:
            metric_by_queue[queue_name] = 0

    return metric_by_queue


if __name__ == '__main__':
    main()
