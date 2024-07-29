from setuptools import find_packages, setup

setup(
    name='jaeger_prometheus_joining',
    packages=find_packages(),
    version='0.2',
    description='Join Jaeger-Traces and Prometheus-Metrics',
    author='Me',
    license='MIT',
    install_requires=['pandas==2.2.2', 'polars==0.20.23', 'pyarrow==17.0.0', 'neo4j==5.20.0', 'neo4jvis==0.0.1', 'ipython==8.26.0', 'logparser3']#, 'neomodel==5.3.0']
)
