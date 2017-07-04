#!/bin/bash

set -e

DBNAME="${DBNAME:=li3ds}"
DBUSER="${DBUSER:=li3ds}"

sudo make install

sudo -u postgres dropdb --if-exists ${DBNAME}
sudo -u postgres createdb -O ${DBUSER} ${DBNAME}
sudo -u postgres psql -d ${DBNAME} -c "create extension plpython2u"
sudo -u postgres psql -d ${DBNAME} -c "create extension postgis"
sudo -u postgres psql -d ${DBNAME} -c "create extension pointcloud"
sudo -u postgres psql -d ${DBNAME} -c "create extension pointcloud_postgis"
sudo -u postgres psql -d ${DBNAME} -c "create extension multicorn"
sudo -u postgres psql -d ${DBNAME} -c "create extension li3ds"

exit 0
