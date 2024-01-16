# ConTest - Parsing

Verarbeitet Prometheus- und Jaeger-Tracing-Daten und versucht diese miteinander zu joinen.

---

## Requirements

* Python 3.11

Parsing der Tracing- und Monitoringdaten:
* Polars
* PyArrow
* ConTest-Tree

Parsing der Logdaten:
* logpai/logparser
* Pandas

Zum Visualisieren der Traces:
* neo4j
* neo4jvis

Für die Docs:
* Pdoc

---

## Packages

* controlflow
  * Bestimmt den Ablauf und die grundsätzliche Orchestrierung.
* featureengineering
  * Fügt neue Daten zu den Grunddaten hinzu bzw ändert diese markant.
* transformationscripts
  * Transformiert bzw. macht etwas mit den Grunddaten. 
* util
  * Visualisierung, Timers, andere Utilityfunktioen

---

## Ablauf

siehe Joinmanager:
1. Finden der Source-Daten 
2. Ausgabe von Statistiken (Wieviel Files, etc)
3. Clearen des Outputfolders 
4. Parsing von den Log-Daten 
5. Parsing von den Prometheus-Daten 
6. Parsing von den Tracing-Daten 
7. Joinen aller Daten 
8. Neue Informationen aus den Daten ziehen 
9. Struktur der Daten ändern (1 Zeile pro Trace)
10. Graphen generieren

## Aufbau der Transformationsklassen

Grundsätzlich sollten fast alle Klassen eine Methode namens ```start()``` sein. 

Im besten Fall ist das auch die einzige Methode, welche public ist. Diese Methode sollte auch nur andere Methoden aufrufen und somit auch nur 'orchestrieren'.

## Ordner-Struktur der Source-Dateien

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

