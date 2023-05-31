#!/bin/sh

echo "pipeline init"

python parsing.py
python burketss.py

echo "done"