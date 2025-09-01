#!/bin/bash

# Clean up Python cache files and other unnecessary files
find . -name "*.pyc" -delete
find . -name "__pycache__" -type d -exec rm -rf {} +
find . -name "*.egg-info" -type d -exec rm -rf {} +
find . -name ".pytest_cache" -type d -exec rm -rf {} +
find . -name ".DS_Store" -delete

# Clean up output folders but preserve directory structure
echo "Cleaning output folders..."
if [ -d "samples/output" ]; then
  # Keep the directory structure but remove files
  find samples/output -type f -not -path "*/\.*" -delete
  echo "Output folders cleaned while preserving directory structure."
else
  echo "Output folder doesn't exist. Nothing to clean."
fi

echo "Project cleaned successfully!"
