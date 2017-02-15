#!/bin/bash
source venv/bin/activate
sudo -u www-data venv/bin/uwsgi --ini uwsgi.ini -s /tmp/public-domain.sock --manage-script-name --mount /publicdomain=main:app
