from setuptools import find_packages, setup

setup(
    name='jaeger_prometheus_joining',
    packages=find_packages(),
    version='0.1.0',
    description='Join Jaeger-Traces and Prometheus-Metrics',
    author='Me',
    license='MIT',
    install_requires=['pandas', 'polars', 'pyarrow']
)
