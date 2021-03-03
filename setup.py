#!/usr/bin/env python
from os.path import abspath, dirname, join
from re import search as re_search
from setuptools import find_packages, setup

PACKAGE_NAME = "tap-ordway"
PACKAGE_DIR = PACKAGE_NAME.replace("-", "_")
ROOT_DIR = abspath(dirname(__file__))

INSTALL_REQUIRES = [
    "singer-python>=5.0.12",
    "requests",
    "kafka-python",
    "inflection",
]

EXTRA_REQUIRES = {
    "dev": ["black==20.8b1", "pylint==2.6.0", "tox==3.20.1"],
    "testing": [
        "mypy==0.790",
        "pytest==6.1.1",
        "pytest-cov==2.10.1",
        "pytest-xdist==2.1.0",
        "vcrpy==4.1.1",
    ],
}

with open(join(ROOT_DIR, "README.md"), encoding="utf-8") as readme_file:
    README = readme_file.read()

with open(
    join(ROOT_DIR, PACKAGE_DIR, "__version__.py"), encoding="utf-8"
) as version_file:
    VERSION = re_search(r'__version__\s+?=\s+?"(.+)"', version_file.read()).group(1)

setup(
    name=PACKAGE_NAME,
    version=VERSION,
    description="Singer.io tap for extracting data",
    long_description=README,
    long_description_content_type="text/markdown",
    author="Stitch",
    url="http://singer.io",
    classifiers=[
        "Programming Language :: Python :: 3 :: Only",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Operating System :: OS Independent",
    ],
    py_modules=["tap_ordway"],
    install_requires=INSTALL_REQUIRES,
    entry_points="""
    [console_scripts]
    tap-ordway=tap_ordway:main
    """,
    packages=find_packages(exclude=["tests", "tests.*"]),
    package_data={"schemas": ["tap_ordway/schemas/*.json"]},
    include_package_data=True,
    extras_require=EXTRA_REQUIRES,
    keywords="singer ordway tap",
)
