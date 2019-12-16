import os
from setuptools import setup
from pyspark_util import __version__


DESCRIPTION = 'PySpark utility functions'

# use README.md as a long description
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    LONG_DESCRIPTION = f.read()


setup(
    name='pyspark-util',
    version=__version__,
    packages=['pyspark_util'],
    python_requires='>=3.5',
    install_requires=[
        'pyspark>=2.4.4',
    ],
    maintainer='harupy',
    maintainer_email='hkawamura0130@gmail.com',
    url='https://github.com/harupy/pyspark-util',
    project_urls={
        'Bug Tracker': 'https://github.com/harupy/pyspark-util/issues',
        'Source Code': 'https://github.com/harupy/pyspark-util'
    },
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
)
