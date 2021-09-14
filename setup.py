"""Cue: Script Orchestration for Data Analysis

Cue lets your package your data analysis into simple actions which can be connected 
into a dynamic data analysis pipeline with coverage over even complex data sets.
"""

DOCLINES = (__doc__ or '').split('\n')

from setuptools import find_packages, setup

setup(
    name='cue',
    package_dir={'cue/cue': 'cue'},
    packages=find_packages(include=['cue']),
    version='0.0.1',
    description=DOCLINES[0],
    long_description="\n".join(DOCLINES[2:]),
    project_urls={
        "Source Code": "https://github.com/ktvng/cue"
    },
    author='ktvng',
    license='MIT',
    python_requires='>=3.8',
    entry_points={
        'console_scripts': {
            'cue=cue.cli:run'
        }
    }
)
