# Required Tooling

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

## First Transformationen

The entry point is always the Jupyter Notebook [control-flow](../../src/control-flow.ipynb), where all parameters can be configured and adjusted. Additionally, the transformation workflow is defined here.

| Parametername              | Description                                                                                                                                                                       | Allowed  input field |
|----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------|
| source                     | Path to the root folder where all files are located (see [Folder structure](folder-struc.md))                                                                                   | $HOME/data/            |
| out                        | Path where all the transformed files are stored                                                                                                                 | ../out                 |
| test_mode                  | Mode in which only a fraction of the data is processed to prevent long runtime                                                                                | True/False             |
| rounding_acc               | All timestamps are rounded to facilitate joining. (see [rounding of timestamps](rounding-identifiers.md))                                                              | 1s, 30s, 1m            |
| save_to_disk               | Parameter that determines whether the files are also written to the disk                                                                                             | True/False             |
| output_vis                 | Parameter that allows debugging outputs of dataframes or schemas to be generated. Note: Some development environments may not handle the amount of output. | True/False             |
| drop_null                  | Parameter that performs a basic filtering. Empty or null metrics are cleaned up before joining, and duplicates are removed.                                 | True/False             |
| additional_name_tracing    | Additional name that is appended to the tracing outputs                                                                                                                      | string                 |
| additional_name_monitoring | Additional name that is appended to the monitoring outputs                                                                                                                   | string                 |


After configuring all parameters, run all cells. (Intellij: Ctrl + Alt + Shift + Enter)

For additional Information: [Description of individual notebooks](documentation.md).
