#!/bin/bash

# Bash wrapper script that runs the generate_all_data.py with a common set of defaults.
echo "Generating sample e-commerce data with default counts..."

python3 generate_all_data.py \
  --customers 25 \
  --products 13 \
  --suppliers 3 \
  --promotions 5 \
  --orders 50

echo "Sample data successfully generated in /sample_data"
