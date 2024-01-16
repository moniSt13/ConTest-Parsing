# Ordner-Struktur der Source-Dateien

Beispielstruktur der Source-Dateien, welche verarbeitet werden:

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