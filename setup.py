import os
from io import open
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
packagemetadata = {}
path = os.path.join(here, "__meta__.py")

with open(file=path, mode="r", encoding="utf-8") as f:
    exec(f.read(), packagemetadata)

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name=packagemetadata["__packageName__"],
    version=packagemetadata["__packageVersion__"],
    description=packagemetadata["__description__"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    license=packagemetadata["__license__"],
    packages=find_packages(include=["aws_beamline_devtools", "aws_beamline_devtools.*"]),
    python_requires=">=3.6",
    install_requires=[
        "botocore~=1.13.25",
        "boto3~=1.10.25",
    ])

# Clean older build: python setup.py clean --all
# Build command : python setup.py bdist_wheel 
# Reinstall : sudo pip3 install --upgrade --force-reinstall  dist/aws_beamline_devtools-0.0.1-py3-none-any.whl