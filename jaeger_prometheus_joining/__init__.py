"""
Main module for parsing Jaeger-traces and Prometheus-metrics.

There are 4 submodules:
* controlflow
* featureengineering
* transformationscripts
* util

The most important for you will be `controlflow`! This module defines the core logic, how the program operates and
parses data."""
from jaeger_prometheus_joining import *
