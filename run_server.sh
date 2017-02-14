#!/bin/bash
source venv/bin/activate
FLASK_APP=main.py python -m flask run --port=8000
