# -*- coding: utf-8 -*-
'''
Graph used in tests (nodes are referentials, edges are transformations)

           +-+                  \
           |1|                   \
           +++                    |
            |                     |
            1                     |
            |                     |
            |                     |
            v                     |
+-+        +-+        +-+         |
|4|<---3---|2|---2--->|3|         |  sensor group 1
+-+        +-+        +-+         |
          / | ↖                   |
         /  |  \                  |
        10  4   9                 |
         \  |  /                  |
          ↘ v /                   |
           +++                    /
           |5|                   /
           +++
            |
            5              <--------------- sensor connection
            |
            |
            v
+-+        +-+       +-+        \
|7|<---6---|6|---8-->|9|         \
+-+        +++       +-+          |
            |                     |
            7                     | sensor group 2
            |                     |
            v                     |
           +++                    |
           |8|                   /
           +-+                  /

'''
import pytest
import psycopg2

from conftest import Database


@pytest.yield_fixture(scope="function")
def db(postgres):
    '''
    Fixture to be used in test functions
    '''
    db = Database(postgres)
    db.execute("set search_path to li3ds, public")
    yield db
    db.conn.rollback()


add_sensor_group1 = '''
    insert into sensor(id, name, serial_number, type)
    values (1, 'cam', 'XKB', 'camera');

    insert into referential (id, sensor, name)
    values (1, 1, 'r1'), (2, 1, 'r2'),
           (3, 1, 'r3'), (4, 1, 'r4'),
           (5, 1, 'r5');

    insert into transfo (id, name, source, target)
    values (1, 't1', 1, 2), (2, 't2', 2, 3),
           (3, 't3', 2, 4), (4, 't4', 2, 5),
           (9, 't9', 5, 2);
'''

add_sensor_group2 = '''
    insert into sensor(id, name, serial_number, type)
    values (2, 'ins', 'INS', 'ins');

    insert into referential (id, sensor, name)
    values (6, 2, 'r6'), (7, 2, 'r7'),
           (8, 2, 'r8'), (9, 2, 'r9');

    insert into transfo (id, name, source, target)
    values (6, 't6', 6, 7),
           (7, 't7', 6, 8), (8, 't8', 6, 9);
'''

add_other_transfos_for_sensor_group2 = '''
    insert into transfo(id, name, source, target)
    values (11, 't11', 6, 7),
           (12, 't12', 6, 8), (13, 't13', 6, 9);
'''

add_sensor_connection = '''
    insert into transfo (id, name, source, target)
    values (5, 't5', 5, 6);

    insert into transfo_tree(id, name, transfos)
    values (3, 't3', ARRAY[5]);
'''

add_transfo_trees = '''
    insert into transfo_tree(id, name, transfos)
    values (1, 't1', ARRAY[1, 2, 3, 4]),
           (2, 't2', ARRAY[6, 7, 8]);
'''

add_another_transfo_tree_for_sensor_group2 = '''
    insert into transfo_tree(id, name, transfos)
    values (4, 't4', ARRAY[11, 12, 13]);
'''

add_platform_config = '''
    insert into platform (id, name) values (1, 'platform');

    insert into platform_config (id, name, platform, transfo_trees)
    values (1, 'p1', 1, ARRAY[1, 2, 3])
'''

add_another_platform_config = '''
    insert into platform_config(id, name, platform, transfo_trees)
    values (2, 'p2', 1, ARRAY[1, 3, 4])
'''

create_test_schema = '''
    create schema test;
'''

create_patch_table = '''
    create table test.patch (id serial, points pcpatch);
'''


def test_schema_li3ds(db):
    assert db.hasschema('li3ds')


def test_table_list(db):
    assert db.hastable('li3ds', 'project')
    assert db.hastable('li3ds', 'session')
    assert db.hastable('li3ds', 'platform')
    assert db.hastable('li3ds', 'platform_config')
    assert db.hastable('li3ds', 'sensor')
    assert db.hastable('li3ds', 'referential')
    assert db.hastable('li3ds', 'datasource')
    assert db.hastable('li3ds', 'processing')
    assert db.hastable('li3ds', 'transfo')
    assert db.hastable('li3ds', 'transfo_type')
    assert db.hastable('li3ds', 'transfo_tree')


def test_check_datasource_uri_bad_scheme_ko(db):
    assert not db.query('''
        select check_datasource_uri('bad:/path/to/file')
    ''')[0][0]


def test_check_datasource_uri_file_ok(db):
    assert db.query('''
        select check_datasource_uri('file:/path/to/file')
    ''')[0][0]


def test_check_datasource_uri_column_bad_format_ko(db):
    db.execute(create_test_schema)
    db.execute(create_patch_table)
    assert not db.query('''
        select check_datasource_uri('column:patch.points')
    ''')[0][0]


def test_check_datasource_uri_column_nonexisting_column_ko(db):
    db.execute(create_test_schema)
    db.execute(create_patch_table)
    assert not db.query('''
        select check_datasource_uri('column:test.patch.foo')
    ''')[0][0]


def test_check_datasource_uri_column_ok(db):
    db.execute(create_test_schema)
    db.execute(create_patch_table)
    assert db.query('''
        select check_datasource_uri('column:test.patch.points')
    ''')[0][0]


def test_check_pcpatch_column_bad_format_ko(db):
    db.execute(create_test_schema)
    db.execute(create_patch_table)
    assert not db.query('''
        select check_pcpatch_column('test.patch');
    ''')[0][0]


def test_check_pcpatch_column_nonexisting_column_ko(db):
    db.execute(create_test_schema)
    db.execute(create_patch_table)
    assert not db.query('''
        select check_pcpatch_column('test.patch.foo');
    ''')[0][0]


def test_check_pcpatch_column_ok(db):
    db.execute(create_test_schema)
    db.execute(create_patch_table)
    assert db.query('''
        select check_pcpatch_column('test.patch.points');
    ''')[0][0]


def test_foreign_key_array_ok(db):
    '''
    Should check constraints on array elements
    '''
    db.execute(add_sensor_group1)
    assert db.query("""select
        foreign_key_array(ARRAY[1, 2, 3, 4], 'li3ds.transfo')""")[0][0]


def test_foreign_key_array_ko(db):
    '''
    Should check constraints on array elements
    '''
    db.execute(add_sensor_group1)
    assert not db.query("""select
        foreign_key_array(ARRAY[1, 6], 'li3ds.transfo')""")[0][0]


def test_check_transfo_exists_constraint_ko(db):
    '''
    Insertion should fail if transformation does not exist
    '''
    # check through constraint
    with pytest.raises(psycopg2.IntegrityError):
        db.execute('''
            insert into transfo_tree(name, transfos)
            values ('t1', ARRAY[1, 7])
        ''')


def test_check_transfo_exists_constraint_ok(db):
    db.execute(add_sensor_group1)
    assert db.rowcount('''
        insert into transfo_tree(name, transfos)
        values ('t1', ARRAY[1, 2, 4])''') == 1


def test_transfo_tree_sensor_ko(db):
    '''should fail if it references a sensor
    and transfos are not connected'''
    db.execute(add_sensor_group1)
    db.execute(add_sensor_group2)
    with pytest.raises(psycopg2.IntegrityError):
        db.execute('''
        insert into transfo_tree (id, name, transfos)
        values (1, 't1', ARRAY[1, 5])''') == 1


def test_isconnected_ok(db):
    db.execute(add_sensor_group1)
    assert db.query("select isconnected(ARRAY[1, 2, 5])")[0][0]


def test_isconnected_with_cycle(db):
    db.execute(add_sensor_group1)
    assert db.query("select isconnected(ARRAY[1, 2, 3, 4, 9])")[0][0]


def test_isconnected_but_duplicate_transfo(db):
    db.execute(add_sensor_group1)
    db.execute("""
        insert into transfo (id, name, source, target)
        values (10, 't1', 2, 5)
    """)
    assert not db.query("select isconnected(ARRAY[1, 2, 3, 4, 9, 10])")[0][0]


def test_isconnected_ko_noconnex(db):
    db.execute(add_sensor_group1)
    db.execute(add_sensor_group2)
    assert not db.query("select isconnected(ARRAY[1, 7])")[0][0]


def test_platform_config_ok(db):
    db.execute(add_sensor_group1)
    db.execute(add_sensor_group2)
    db.execute(add_transfo_trees)
    db.execute(add_sensor_connection)
    assert db.rowcount(add_platform_config) == 1


def test_platform_config_ko(db):
    db.execute(add_sensor_group1)
    db.execute(add_sensor_group2)
    db.execute(add_transfo_trees)
    with pytest.raises(psycopg2.IntegrityError):
        db.execute(add_platform_config)


def test_check_transfotree_istree_empty_ok(db):
    assert db.query("select check_transfotree_istree(NULL)")[0][0]


def test_check_transfotree_istree_connex(db):
    db.execute(add_sensor_group1)
    db.execute(add_sensor_group2)
    db.execute(add_sensor_connection)
    db.execute(add_transfo_trees)
    assert db.query("select check_transfotree_istree(ARRAY[1, 2, 3])")[0][0]


def test_check_transfotree_istree_noconnex(db):
    db.execute(add_sensor_group1)
    db.execute(add_sensor_group2)
    db.execute(add_transfo_trees)
    assert not db.query("select check_transfotree_istree(ARRAY[1, 2])")[0][0]


def test_dijkstra_function(db):
    db.execute(add_sensor_group1)
    db.execute(add_sensor_group2)
    db.execute(add_transfo_trees)
    db.execute(add_sensor_connection)
    db.execute(add_platform_config)
    assert db.query("select dijkstra(1, 1, 2)")[0][0] == [1]
    assert db.query("select dijkstra(1, 1, 5)")[0][0] == [1, 4]
    assert db.query("select dijkstra(1, 1, 7)")[0][0] == [1, 4, 5, 6]
    assert db.query("select dijkstra(1, 3, 8)")[0][0] == []
    assert db.query("select dijkstra(1, 1, 1)")[0][0] == []
    assert db.query("select dijkstra(1, 5, 4)")[0][0] == []


def test_dijkstra_function_with_two_platform_configs(db):
    db.execute(add_sensor_group1)
    db.execute(add_sensor_group2)
    db.execute(add_other_transfos_for_sensor_group2)
    db.execute(add_transfo_trees)
    db.execute(add_sensor_connection)
    db.execute(add_another_transfo_tree_for_sensor_group2)
    db.execute(add_platform_config)
    db.execute(add_another_platform_config)
    assert db.query("select dijkstra(2, 1, 2)")[0][0] == [1]
    assert db.query("select dijkstra(2, 1, 5)")[0][0] == [1, 4]
    assert db.query("select dijkstra(2, 1, 7)")[0][0] == [1, 4, 5, 11]
    assert db.query("select dijkstra(2, 3, 8)")[0][0] == []
    assert db.query("select dijkstra(2, 1, 1)")[0][0] == []
    assert db.query("select dijkstra(2, 5, 4)")[0][0] == []


def test_dijkstra_function_exception(db):
    db.execute(add_sensor_group1)
    db.execute(add_sensor_group2)
    db.execute(add_transfo_trees)
    db.execute(add_sensor_connection)
    db.execute(add_platform_config)
    with pytest.raises(Exception):
        db.query("select dijkstra(1, 1, 55)")
    with pytest.raises(Exception):
        db.query("select dijkstra(1, 55, 1)")


def test_dijkstra_findref(db):
    db.execute(add_sensor_group1)
    db.execute(add_sensor_group2)
    db.execute(add_transfo_trees)
    db.execute(add_sensor_connection)
    db.execute(add_platform_config)
    assert db.query("select dijkstra(1, 1, 8, 'ins')")[0][0] == [6]


def test_dijkstra_findref_ko(db):
    db.execute(add_sensor_group1)
    db.execute(add_sensor_group2)
    db.execute(add_transfo_trees)
    db.execute(add_sensor_connection)
    db.execute(add_platform_config)
    with pytest.raises(Exception):
        db.query("select dijkstra(1, 1, 8, 'lidar')")[0][0]


# FIXME activate when delete triggers will be ready
# def test_delete_transfo_cascade(db):
#     '''deleting a transfo should propagate deletion of related platform_config
#     and transfo_trees'''
#     db.execute(add_sensor_group1)
#     db.execute(add_sensor_group)
#     db.execute(add_transfo_trees)
#     db.execute(add_platform_config)
#     db.execute("delete from transfo where id = 1")
#     assert db.rowcount('''
#         select 1 from transfo_tree where array[1] <@ transfos
#         ''') == 0, db.show_table('transfo_tree')
#     assert db.rowcount('''
#         select 1 from platform_config pf
#         join transfo_tree tt on array[tt.id] <@ pf.transfo_trees
#         where array[1] <@ tt.transfos
#         ''') == 0, db.show_table('platform_config')
