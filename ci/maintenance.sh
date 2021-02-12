#!/bin/sh
set -ex

echo "Create DynamoDB table(s)"
python3 commands/create_table.py
touch .done

tail -f /dev/null