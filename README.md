# ConTest - Parsing

Merges together Logs, traces, and monitoring data to get them into to format of one sample per line over all microservices.


#### How can I build pdocs?
```
pdoc ./jaeger_prometheus_joining -o ./pdocs
```
An HTML file should be generated in the 'pdocs'-folder.

---

## Requirements

* Python 3.11

Parsing of traces (Jaeger), and metrics (Prometheus) data:
* Polars
* PyArrow
* ConTest-Tree

Parsing der Logdaten:
* logpai/logparser
* Pandas

For visualization of traces:
* neo4j
* neo4jvis

For the documentation:
* Pdoc

---

## Packages

* controlflow
  * Is responsible for the sequences and the basic orchestration.
* featureengineering
  * Adds new data to the basic data and changes them accordingly.
* transformationscripts
  * transformes the basic data. 
* util
  * Visualization, Timers, other Utilityfunctions
  * 
---

## Sequences of data transformation to final output

see Joinmanager:
1. Finding of source data 
2. Show statistics about identified source data
3. Clear Output Folder in case this was there for the previous run 
4. Parsing of Log-Data 
5. Parsing of metric-Data (Prometheus)
6. Parsing of tracing-Data (Jaeger)
7. Join of all data 
8. create insights from joined data 
9. change structure of data for use in PLS-SEM model (1 line per trace)
10. generate graph of tracing information and merged information from logs and metrics

## Structure der Transformationclasses

Every class should start with the method ```start()``` 

In the best case, it should be the only method, that is public. This method should call other methods and only be used for the 'orchestration'.

## Folder-Structure of the source data

Example of a folder structure of the source data, which this Code is able to transform:

```
.
└── Data_TrainTicket/
    ├── ts-admin-basic-info-service-sprintstarterweb_1.5.22/
    │   ├── Monitoring/
    │   │   ├── json_container_cpu_usage_seconds_total.json
    │   │   ├── json_container_processes.json
    │   │   ├── json_container_spec_cpu_shares.json
    │   │   └── ...
    │   └── Traces/
    │       ├── ts-order-service.json
    │       ├── ts-basic-service.json
    │       ├── ts-inside-payment-service.json
    │       └── ...
    ├── ts-auth-mongo_4.4.15/
    │   ├── Monitoring/
    │   │   └── ...
    │   └── Traces/
    │       └── ...
    ├── ts-auth-mongo_4.4.15/
    │   ├── Monitoring/
    │   │   └── ...
    │   └── Traces/
    │       └── ...
    ├── ts-auth-mongo_5.0.9/
    │   ├── Monitoring/
    │   │   └── ...
    │   └── Traces/
    │       └── ...
    ├── ts-order-service_2.7.1-SpringBootStarterParent/
    │   ├── Monitoring/
    │   │   └── ...
    │   └── Traces/
    │       └── ...
    ├── ts-order-service_springstarterdataMongoDB_2.0.0.RELEASE/
    │   ├── Monitoring/
    │   │   └── ...
    │   └── Traces/
    │       └── ...
    └── etc...
```

## Possible Configurations

With the help of the class '''ParseSettings''' you can change the configurations, and you have the following options:

* source
* out
* test_mode
* rounding_acc
* save_to_disk
* output_vis
* drop_null
* additional_name_tracing
* additional_name_metrics
* final_name_suffix
* clear_output
* print_statistics
* visualize_graph
* neo4j_uri
* neo4j_container_name
* tree_settings
  * print_data
  * print_data_with_accessing_field
  * accessing_field
 

  
#### Old Documentation can be found under the following links:
* [All scripts and their execution, when we started as Jupyter notebook](meta/wiki/old/documentation.md)
* [folder structure without Logs](meta/wiki/old/folder-struc.md)
* [old description of parameters and the necessary tooling](meta/wiki/old/get-started.md)
* [Insights/Problems/Workarounds](meta/wiki/old/insights-experience.md)
* [Where rounding is necessary](meta/wiki/old/rounding-identifiers.md)

