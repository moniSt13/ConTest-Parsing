# Insights

### Timestamps

Prometheus collects its metrics at regular intervals. In contrast, Jaeger traces can be either very long and detailed or very short. As a result, it can happen that a Jaeger trace falls between Prometheus' measurement intervals. Generally, it will not be common for these timestamps to align perfectly. Therefore, timestamps need to be rounded or joined using an as-of join. If the exported data is in a completely different time window, logically, no results will be obtained.

### Container, Service, Pod

Prometheus has several labels:

* container
* endpoint
* job
* namespace
* node
* pod
* service

Jaeger too:

* servicename
* hostname
* ip
* component

The meaning of which labels can vary significantly depending on the configuration. In this case, the hostname and the pod were compared.

### Container = 'POD' oder null

In many Prometheus entries (or often exactly 50 percent), you will find a container named "POD." These 'POD' containers are known as 'pause' containers. They essentially act as the 'parent' container, sharing the Linux namespace with other containers and so on. More specifically, the pause container is used to hold the network namespace for the pod, providing isolation and managing network resources for the other containers within the pod. [please see there](https://www.ianlewis.org/en/almighty-pause-container). However, quite often this data might not be relevant, thus we filtered them.

### Duplicate Traces

Since a trace can span multiple services, these traces will also appear multiple times in the respective exports of these services. They need to be filtered out. In other words, the data might seem larger than it actually is.

### Format of metrics

A very significant issue was the format of the metrics. Prometheus records them in the following way:

| __name__        | values                          | container | ... |
|-----------------|---------------------------------|-----------|-----|
| cpu_usage_total | [[1, 1], [2, 1], [3, 1], ...]   | abcd1234  | ... |
| cpu_usage_total | [[1, 1], [2, 1.5], [3, 1], ...] | efgh5678  | ... |

In the end, however, the desired format should be one in which the data is viewed as columns:

| container | measure_time | cpu_usage_total | ... |
|-----------|--------------|-----------------|-----|
| abcd1234  | 1            | 1               | ... |
| abcd1234  | 2            | 1               | ... |
| abcd1234  | 3            | 1               | ... |
| efgh5678  | 1            | 1               | ... |
| efgh5678  | 2            | 1.5             | ... |
| efgh5678  | 3            | 1               | ... |

### Merging of data

Some data overlaps significantly better than others, not just because there is more data available. For instance, some data might not overlap at all. In the service ts-admin-basic-info-service, there are over 2400 metrics in large quantities. Each metric can be found and joined. In contrast, in ts-auth-mongo-4.4.15, there is overlap only with container_memory_working_set_bytes and container_cpu_usage_seconds_total. However, upon closer inspection, there is also a 22 MB file for the metric container_network_transmit_packets_total. Yet, this file yields no results, which is a bit puzzling.

This file was examined, and out of the 645,817 entries:

* 47046 null
* 598771 with container: "POD"

To summarise this two: 645817

If only the container is null and the pod is not, these can still be further processed. However, in the following screenshot, you can see that this will not work:
![img.png](container_network_transmit_packets_total_STATISTICS.png.png)

### Zahlen und Fakten:

| Service                                                             | Monitoringdaten | Tracingdaten | # Spalten | # Zeilen |
|---------------------------------------------------------------------|-----------------|--------------|-----------|----------|
| ts-auth-mongo_5.0.9_2022-07-06                                      | 51.205784MB     | 0.17561MB    | 10        | 37       |
| ts-auth-mongo_4.4.15_2022-07-13                                     | 49.291915MB     | 3.702373MB   | 14        | 377      |
| ts-order-service_3.0.4-mongodb-driver_2022-07-13                    | 48.1162MB       | 24.866914MB  | 14        | 397      |
| ts-order-service_mongodb_4.2.2_2022-07-12                           | 48.076004MB     | 45.314627MB  | 14        | 1283     |
| ts-admin-basic-info-service-sprintstarterweb_1.5.22                 | 1733.028127MB   | 33.190224MB  | 61        | 976      |
| ts-order-service_2.7.1-SpringBootStarterParent_2022-07-11           | 54.617123MB     | 37.246728MB  | 16        | 2876     |
| ts-order-service_springstarterdataMongoDB_1.5.22.RELEASE_2022-07-11 | 54.76274MB      | 84.725155MB  | 16        | 2643     |
| ts-order-service_springstarterdataMongoDB_2.0.0.RELEASE_2022-07-11  | 54.803428MB     | 30.214909MB  | 16        | 1455     |
| ts-order-service_mongodb_4.4.15_2022-07-12                          | 49.008528MB     | 20.715173MB  | 14        | 1304     |

The dataset has 10 columns if no joins can be performed at all. There are 16 columns for the 4 metrics that could be joined.

The correlation matrix (Pearson) that results is as follows:

|            | monitoring | tracing | col    | row    |
|------------|------------|---------|--------|--------|
| monitoring | 1          | 0.033   | 0.993  | -0.105 |
| tracing    | 0.033      | 1       | 0.108  | 0.771  |
| col        | 0.993      | 0.108   | 1      | -0.017 |
| rows       | -0.105     | 0.771   | -0.017 | 1      |


 ![](/home/michaelleitner/Documents/contest/ConTest-Parsing/meta/ColRowsToTraceMetrics.png)
