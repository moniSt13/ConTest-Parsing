"""
This module has 2 classes:
* JoinManager
* ParseSettings

The latter is responsible for configuring the system to your needs. It handles things like input, output,
naming of files and even more. Please look at the documentation of the class itself to gain further insight.

On the other hand `JoinManager` defines the order of every operation and which files are parsed. Most of the
transformation-heavy work is outsourced and this class calls the transformationscripts."""
