#!/bin/bash
source venv/bin/activate
sudo -u www-data venv/bin/uwsgi --uid www-data --gid www-data -s /tmp/public-domain.sock --manage-script-name --mount /publicdomain=main:app
