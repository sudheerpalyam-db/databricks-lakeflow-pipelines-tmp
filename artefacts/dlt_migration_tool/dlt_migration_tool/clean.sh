#!/bin/bash

# Clean up Python cache files and other unnecessary files
find . -name "*.pyc" -delete
find . -name "__pycache__" -type d -exec rm -rf {} +
find . -name "*.egg-info" -type d -exec rm -rf {} +
find . -name ".pytest_cache" -type d -exec rm -rf {} +
find . -name ".DS_Store" -delete

echo "Project cleaned successfully!"
