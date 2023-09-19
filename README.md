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

---

#### README minimal outdated (TODO)! 
* Mehr Settings
* Keine Jupyter-Notebooks, sondern Python-Klassen
* Implementierung minimal optimiert
* Erkenntnisse blieben gleich!





