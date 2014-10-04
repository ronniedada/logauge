"""setup.py script for Logauge"""

from setuptools import setup, find_packages

version = '0.0.1'

setup(
    name='logauge',
    version=version,
    description='Logauge',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'pyyaml==3.1.1',
        'twisted==14.0.2'
    ],
    entry_points={
        'console_scripts': [
            'logauge-worker = logauge.worker:main',
            'logauge-controller = logauge.controller:main'
        ]
    },
)
