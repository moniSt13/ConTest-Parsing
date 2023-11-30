# Benötigtes Tooling

#### Minimal

* python 3.11 (3.12 doesnt work)
    * jupyter
    * pandas
    * polars
    * pyarrow

#### With graph-visualization

* python 3.11
    * jupyter
    * pandas
    * polars
    * pyarrow
    * neo4j
    * neo4jvis
* docker
    * neo4j container

___

## Erste Transformationen

Entrypoint ist immer das Jupyter Notebook [control-flow](../../src/control-flow.ipynb), in welchem alle Parameter
konfiguriert und angepasst werden können. Weiters wird hier der Ablauf der Transformation festgelegt.

| Parametername              | Beschreibung                                                                                                                                                                       | Erlaubte Eingabefelder |
|----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------|
| source                     | Pfad zum Root-Ordner, in welchem alle Dateien liegen (siehe [Ordnerstruktur](./folder-struc.md))                                                                                   | $HOME/data/            |
| out                        | Pfad, in welchem alle transformierten Dateien abgelegt werden                                                                                                                      | ../out                 |
| test_mode                  | Modus, in welchem nur ein Bruchteil von Daten hergezogen werden, um lange Laufzeiten zu verhindern                                                                                 | True/False             |
| rounding_acc               | Alle Zeitstempel werden gerundet, um das Joinen anzugeben (siehe [Runden der Zeitstempel](./rounding-identifiers.md))                                                              | 1s, 30s, 1m            |
| save_to_disk               | Parameter, welcher bestimmt, ob die Dateien auch auf die Festplatte geschrieben werden                                                                                             | True/False             |
| output_vis                 | Parameter, mit welchem Debugging-Outputs von Dataframes oda Schemen ausgegeben werden können. Achtung: Gewisse Entwicklungsumgebungen können die Menge an Output nicht verarbeiten | True/False             |
| drop_null                  | Parameter, welcher eine gewisse Grundfilterung vornimmt. Leere oder null Metriken werden schon vor dem Joinen bereinigt. Duplikate werden entfernt                                 | True/False             |
| additional_name_tracing    | Zusätzlicher Name, welcher den Tracing-Outputs angehängt wird                                                                                                                      | string                 |
| additional_name_monitoring | Zusätzlicher Name, welcher den Monitoring-Outputs angehängt wird                                                                                                                   | string                 |


Nach dem Konfigurieren aller Parameter alle Zellen durchlaufen. (Intellij: Ctrl + Alt + Shift + Enter)


Für weitere Infos gibt es hier eine [Erklärung der einzelnen Notebooks](documentation.md).
