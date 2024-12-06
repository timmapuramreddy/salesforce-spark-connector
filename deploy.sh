#!/bin/bash

# Build the package
python setup.py sdist bdist_wheel

# Upload to PyPI (you'll need to have twine installed)
twine upload dist/* 