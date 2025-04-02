#!/bin/bash

# Create directory if it doesn't exist
mkdir -p .python_packages/lib/site-packages

# Install packages
pip install beautifulsoup4==4.12.2 bs4==0.0.1 selenium==4.16.0 pyodbc==4.0.39 -t ./.python_packages/lib/site-packages

# List installed packages
echo "Installed packages:"
ls -la ./.python_packages/lib/site-packages
