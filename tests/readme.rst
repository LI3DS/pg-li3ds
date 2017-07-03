Testing
=======

Testing requires Python2, since the plpython2u extension needs to be compiled for PostgreSQL.

We use pytest for testing in addition to a temporary PostgreSQL database thanks to pyembedpg.

Dependencies: check that gcc and readline (e.g., libreadline-dev on Debian, readline on MacOS) are installed.

Start a dedicated Python virtualenv, and install dependencies::

    pip install -r requirements.txt

Run tests::

    py.test -v

The first time should be a little bit slow because PostgreSQL, pgPointCloud and PostGIS extensions
will be downloaded and installed in a local cache directory.

Each time you launch tests, a database will be created and destroyed at the end of the tests.
