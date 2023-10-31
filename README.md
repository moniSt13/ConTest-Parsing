# ConTest - Parsing - Leitner

Jupyter Notebooks, um Jaeger-Traces und Prometheus-Daten zu joinen. Dabei wurden die Prometheus-Daten von der HTTP-API
abgefragt und liegen im JSON-Format
vor. ([Prometheus-Schema](https://prometheus.io/docs/prometheus/latest/querying/api/)
und [Jaeger-Schema](https://www.jaegertracing.io/docs/1.48/apis/))

Es werden dabei die Daten mithilfe des Podnamens gejoint mithilfe eines Inner-Joins. Dabei können viele Daten verloren
gehen,
jedoch bietet sich ein Left-Join auch nicht an, da es bei über 2000 Prometheus-Metriken sehr viele Null-Felder gibt.

* [Get started!](meta/wiki/get-started.md)
* [Funktionsweise und einzelne Notebooks](meta/wiki/documentation.md)
* [Erkenntnisse über Daten](meta/wiki/insights-experience.md)

#### Beispielverwendung:
```
from jaeger_prometheus_joining.controlflow.JoinManager import JoinManager


def main():
    manager = JoinManager()
    manager.settings.source = "/home/gepardec/Documents/contest/Data_TrainTicket"
    manager.settings.test_mode = False
    manager.settings.tree_settings.print_data_with_accessing_field = True
    manager.settings.tree_settings.accessing_field = 1
    manager.settings.out = "./out"
    manager.process()


main()
```

---

#### README minimal outdated (TODO)! 
* Höhere Anzahl an Settings
* Keine Jupyter-Notebooks, sondern Python-Klassen
* Implementierung minimal optimiert
* Erkenntnisse blieben gleich!

#### TODO

* Traces zur selben Zeit mergen die Daten, so wenig leere Spalten wie möglich
* 





