import os
import tempfile
import tarfile
import shutil
import subprocess
from pathlib import Path

import requests
import psycopg2
import pytest
from tabulate import tabulate
from pyembedpg import PyEmbedPg, PyEmbedPgException


POSTGRES_VERSION = '9.6.3'
POSTGIS_VERSION = 'ff0a844e606622f45841fc25221bbaa136ed1001'  # 2017/05/31
POSTGIS_URL = (
    'https://github.com/postgis/postgis/archive/{}.tar.gz'
    .format(POSTGIS_VERSION))

POINTCLOUD_VERSION = 'master'  # dev branch of li3ds org
POINTCLOUD_URL = (
    'https://github.com/LI3DS/pointcloud/archive/{}.tar.gz'
    .format(POINTCLOUD_VERSION))

EXTENSION_DIR = str((Path(__file__).parent.parent).resolve())


@pytest.fixture(scope="session")
def postgres(request):
    def endup():
        print("Database shutdown")
        pg.shutdown()
    request.addfinalizer(endup)

    pg = PyEmbedPg(POSTGRES_VERSION, config_options='--with-python').start(15432)
    pg.create_database('testdb')

    bin_dir = os.path.dirname(pg._postgres_cmd)
    # add bin dir to path to find the local pg_config
    env = os.environ.copy()
    env['PATH'] = '{}:{}'.format(bin_dir, env['PATH'])

    # install dependencies
    install_postgis(env)
    install_pointcloud(env)
    # install li3ds extension inside temp database
    subprocess.check_output(['make', 'install'], env=env, cwd=EXTENSION_DIR)

    load_extensions(pg)

    return pg


def load_extensions(pg):
    with psycopg2.connect(
            host='/tmp/',
            database='testdb',
            user=pg.ADMIN_USER,
            port=pg.running_port) as conn:
        with conn.cursor() as cursor:
            cursor.execute('CREATE extension postgis')
            cursor.execute('CREATE extension plpython3u')
            cursor.execute('CREATE extension pointcloud')
            cursor.execute('CREATE extension pointcloud_postgis')
            cursor.execute('CREATE extension li3ds')


def install_postgis(env):
    home_dir = os.path.expanduser("~")
    cache_dir = os.path.join(home_dir, PyEmbedPg.CACHE_DIRECTORY)
    postgis_dir = 'postgis-{}'.format(POSTGIS_VERSION)
    target_dir = os.path.join(cache_dir, postgis_dir)
    #  if the version we want to download already exists, do not do anything
    if os.path.exists(target_dir):
        print('Postgis Version {} already present in cache'.format(POSTGIS_VERSION))
        return

    response = requests.get(POSTGIS_URL, stream=True)

    if not response.ok:
        raise PyEmbedPgException(
            'Cannot download file {url}. Error: {error}'
            .format(url=POSTGIS_URL, error=response.content))

    with tempfile.NamedTemporaryFile() as fd:
        print('Downloading {url}'.format(url=POSTGIS_URL))
        for block in response.iter_content(chunk_size=4096):
            fd.write(block)
        fd.flush()
        # Unpack the file into temporary dir
        temp_dir = tempfile.mkdtemp()
        source_dir = os.path.join(temp_dir, postgis_dir)
        try:
            # Can't use with context directly because of python 2.6
            with tarfile.open(fd.name) as tar:
                tar.extractall(temp_dir)
            subprocess.check_call(
                'cd {path} && '
                './autogen.sh && '
                './configure --prefix={target_dir} --with-pgconfig={cache_dir}/{postgres_version}/bin/pg_config && '
                'make install'
                .format(path=source_dir, target_dir=target_dir, cache_dir=cache_dir, postgres_version=POSTGRES_VERSION),
                shell=True, env=env
            )
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


def install_pointcloud(env):
    home_dir = os.path.expanduser("~")
    cache_dir = os.path.join(home_dir, PyEmbedPg.CACHE_DIRECTORY)
    pointcloud_dir = 'pointcloud-{}'.format(POINTCLOUD_VERSION)
    target_dir = os.path.join(cache_dir, pointcloud_dir)
    #  if the version we want to download already exists, do not do anything
    if os.path.exists(target_dir):
        print('pgPointCloud already present in cache')
        return

    response = requests.get(POINTCLOUD_URL, stream=True)

    if not response.ok:
        raise PyEmbedPgException(
            'Cannot download file {url}. Error: {error}'
            .format(url=POINTCLOUD_URL, error=response.content))

    with tempfile.NamedTemporaryFile() as fd:
        print('Downloading {url}'.format(url=POINTCLOUD_URL))
        for block in response.iter_content(chunk_size=4096):
            fd.write(block)
        fd.flush()
        # Unpack the file into temporary dir
        temp_dir = tempfile.mkdtemp()
        source_dir = os.path.join(temp_dir, pointcloud_dir)
        try:
            # Can't use with context directly because of python 2.6
            with tarfile.open(fd.name) as tar:
                tar.extractall(temp_dir)
            subprocess.check_call(
                'cd {path} && '
                './autogen.sh &&  '
                './configure --prefix={target_dir} --with-pgconfig={cache_dir}/{postgres_version}/bin/pg_config && '
                'make &&'
                'make install'
                .format(path=source_dir, target_dir=target_dir, cache_dir=cache_dir, postgres_version=POSTGRES_VERSION),
                shell=True, env=env
            )
            # empty dir since pointcloud doesn't have binaries but usefull to cache
            os.mkdir(target_dir)
        finally:
            # pass
            shutil.rmtree(temp_dir, ignore_errors=True)


class Database:
    """
    Database object used to provide some useful functions
    """

    def __init__(self, postgres):
        self.conn = psycopg2.connect(
            host='/tmp/',
            database='testdb',
            user=postgres.ADMIN_USER,
            port=postgres.running_port)

    def query(self, request):
        with self.conn.cursor() as cursor:
            cursor.execute(request)
            return cursor.fetchall()

    def execute(self, request):
        with self.conn.cursor() as cursor:
            cursor.execute(request)

    def rowcount(self, request):
        with self.conn.cursor() as cursor:
            cursor.execute(request)
            return cursor.rowcount

    def hasschema(self, schemaname):
        return self.rowcount(
            "select 1 from pg_namespace where nspname = '{}'"
            .format(schemaname)) > 0

    def hastable(self, schemaname, tablename):
        return self.rowcount(
            "select 1 from pg_tables where tablename = '{}' and schemaname='{}'"
            .format(tablename, schemaname)) > 0

    def show_table(self, tablename):
        with self.conn.cursor() as cursor:
            cursor.execute("select * from {}".format(tablename))
            columns = [col.name for col in cursor.description]
            results = cursor.fetchall()

        return '\n' + tabulate(
            results,
            tablefmt='orgtbl',
            headers=columns
        )
