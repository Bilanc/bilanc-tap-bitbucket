#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="tap-bitbucket",
    version="1.10.5",
    description="Singer.io tap for extracting data from the GitHub API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap-bitbucket"],
    install_requires=[
        "singer-python==5.12.1",
        "requests==2.29.0",
        "urllib3==1.26.20",
        "backoff==1.8.0",
        "cryptography==37.0.4",
        "pyjwt==2.4.0",
    ],
    extras_require={"dev": ["pylint==2.6.2", "ipdb", "nose", "requests-mock==1.9.3"]},
    entry_points="""
          [console_scripts]
          tap-bitbucket=tap_bitbucket:main
      """,
    packages=["tap_bitbucket"],
    package_data={"tap_bitbucket": ["tap_bitbucket/schemas/*.json"]},
    include_package_data=True,
)
