import requests
from urllib.parse import urlencode

# PROMETHEUS_URL = 'http://localhost:9090/api/v1/query_range'
PROMETHEUS_URL = 'http://prometheus-operated.kube-prometheus.svc.cluster.local:9090/api/v1/query_range'


def test_prometheus_connection():
    try:
        response = requests.get(
            PROMETHEUS_URL.replace("/query_range", "/targets"))
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print("Error connecting to Prometheus:", e)
        exit(1)


def fetch_prometheus_metrics(witness_file, start_time, end_time):
    filters = '{namespace="zk-evm", pod="zk-evm-worker-.*"}'
    queries = {
        # CPU usage
        # Total CPU usage of all zk-evm worker pods in the cluster.
        'cluster_cpu_usage': f'avg(rate(container_cpu_usage_seconds_total{filters}[1m]) * 100) by (pod)',
        # Average CPU usage per zk-evm worker pods.
        'pod_cpu_usage': f'sum(rate(container_cpu_usage_seconds_total{filters}[1m]) * 100)',

        # Memmory usage
        # Total memory usage of all zk-evm worker pods in the cluster.
        'cluster_memory_bytes': f'sum(container_memory_usage_bytes{filters})',
        # Average memory usage per zk-evm worker pod.
        'pod_memory_bytes': f'avg(container_memory_usage_bytes{filters}) by (pod)',

        'disk_read': 'node_disk_read_bytes_total',
        'disk_write': 'node_disk_written_bytes_total',

        # Network receive/transmit
        'cluster_network_receive': f'avg(container_network_receive_bytes_total{filters}) by (pod)',
        'pod_network_receive': f'sum(container_network_receive_bytes_total{filters})',

        'cluster_network_transmit': f'avg(container_network_transmit_bytes_total{filters}) by (pod)',
        'pod_network_transmit': f'sum(container_network_transmit_bytes_total{filters})',

        # OutOfMemory (OOM) events
        'pod_oom_events': f'sum(container_oom_events_total{filters}) by (pod)',

        # Number of processes
        # sum(container_processes{namespace="zk-evm", pod=~"zk-evm-worker-.*"}) by (pod)
        'pod_processes': f'sum(container_processes{filters}) by (pod)',
    }

    metrics = []
    for name, query in queries.items():
        start_str = start_time.replace(microsecond=0).isoformat() + "Z"
        end_str = end_time.replace(microsecond=0).isoformat() + "Z"
        params = {
            'query': query,
            'start': start_str,
            'end': end_str,
            'step': '10s'
        }
        url = PROMETHEUS_URL + '?' + urlencode(params)
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        metrics.append((name, data['data']['result']))
    return metrics


def log_metrics_to_csv(witness_file, metrics):
    import csv
    import os
    from datetime import datetime

    starting_block = os.path.basename(
        witness_file).replace('.witness.json', '')
    with open('metrics.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        for metric_name, metric_data in metrics:
            row = [starting_block, datetime.now(), metric_name]
            for metric in metric_data:
                values = [value[1] for value in metric['values']]
                row.extend(values)
            writer.writerow(row)
