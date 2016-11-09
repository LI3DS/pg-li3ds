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
            ^                     |
            |                     |
            4                     |
            |                     |
            |                     |
           +++                   /
           |5|                  /
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


transfos_sample = '''
    insert into referential (id) values (1), (2), (3), (4), (5);
    insert into transfo (id, source, target)
    values (1, 1, 2), (2, 2, 3), (3, 2, 4), (4, 5, 2);
'''


add_sensor_group = '''
    insert into referential (id) values (6), (7), (8), (9);
    insert into transfo (id, source, target)
    values (5, 5, 6), (6, 6, 7), (7, 6, 8), (8, 6, 9);
'''

add_transfo_trees = '''
    insert into platform (id, name) values (1, 'platform');
    insert into transfo_tree(id, transfos) values (1, ARRAY[1, 2, 3, 4]);
    insert into transfo_tree(id, transfos) values (2, ARRAY[6, 7, 8]);
    insert into transfo_tree(id, sensor_connections, transfos) values (3, true, ARRAY[5]);
'''

add_platform_config = '''
    insert into platform_config (id, platform, transfo_trees)
    values (1, 1, ARRAY[1, 2, 3])
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
    assert db.hastable('li3ds', 'posdatasource')
    assert db.hastable('li3ds', 'posprocessing')
    assert db.hastable('li3ds', 'transfo')
    assert db.hastable('li3ds', 'transfo_type')
    assert db.hastable('li3ds', 'transfo_tree')


def test_create_project(db):
    '''
    When create_project is invoked, a new schema must be created in addition to
    a new table and an entry in project table
    '''
    db.execute("select create_project('paris', 'Europe/Paris')")
    assert db.hasschema("paris")
    assert db.query("select name from project") == [('paris',)]
    assert db.hastable('paris', "image")


def test_create_project_dup(db):
    db.execute("select create_project('paris', 'Europe/Paris')")
    with pytest.raises(psycopg2.IntegrityError):
        db.execute("select create_project('paris', 'Europe/Paris')")


def test_create_project_extent_badsrid(db):
    with pytest.raises(psycopg2.DataError):
        db.execute('''
            select create_project(
            'paris',
            'Europe/Paris',
            'srid=4325;polygon((1 1, 2 2, 3 3, 1 1))'
        )''')


def test_delete_project(db):
    db.execute("select delete_project('paris')")
    assert not db.hasschema("paris")


def test_delete_project_cascading(db):
    '''
    Deleting a project should delete all related sessions,
    datasource and posdatasource but not platform related objects
    '''
    pid = db.query("select create_project('paris', 'Europe/Paris')")[0][0]
    db.execute("""
        insert into platform (id, name) values (1, 'platform');
        insert into session(id, name, project, platform) values (1, 'session', {}, 1);
        insert into referential (id) values (1);
        insert into datasource (id, session, referential) values (1, 1, 1);
        insert into sensor (id, serial_number, type) values (1, '', 'ins');
        insert into posdatasource (id, session, sensor) values (1, 1, 1);
    """.format(pid))
    db.execute("select delete_project('paris')")
    assert db.rowcount("select * from session") == 0
    assert db.rowcount("select * from datasource") == 0
    assert db.rowcount("select * from posdatasource") == 0
    assert db.rowcount("select * from platform") == 1
    assert db.rowcount("select * from referential") == 1
    assert db.rowcount("select * from sensor") == 1


def test_check_transfo_exists_constraint_ko(db):
    '''
    Insertion should fail if transformation does not exist
    '''
    db.execute(transfos_sample)
    # check through constraint
    with pytest.raises(psycopg2.IntegrityError):
        db.execute("insert into transfo_tree(transfos) values (ARRAY[1, 7])")


def test_check_transfo_exists_constraint_ok(db):
    db.execute(transfos_sample)
    assert db.rowcount("insert into transfo_tree(transfos) values (ARRAY[1, 2, 4])") == 1


def test_transfo_tree_sensor_connection_ok(db):
    db.execute(transfos_sample)
    db.execute(add_sensor_group)
    assert db.rowcount('''
        insert into transfo_tree (id, sensor_connections, transfos)
        values (1, true, ARRAY[5])''') == 1


def test_transfo_tree_sensor_connection_ko(db):
    '''should fail if it's not a sensor_connection and transfos are not connected'''
    db.execute(transfos_sample)
    db.execute(add_sensor_group)
    with pytest.raises(psycopg2.IntegrityError):
        db.execute('''
        insert into transfo_tree (id, sensor_connections, transfos)
        values (1, false, ARRAY[1, 5])''') == 1


def test_foreign_key_array_ok(db):
    '''
    Should check constraints on array elements
    '''
    db.execute(transfos_sample)
    assert db.query("select foreign_key_array(ARRAY[1, 2, 3, 4], 'li3ds.transfo')")[0][0]


def test_foreign_key_array_ko(db):
    '''
    Should check constraints on array elements
    '''
    db.execute(transfos_sample)
    assert not db.query("select foreign_key_array(ARRAY[1, 6], 'li3ds.transfo')")[0][0]


def test_check_istree_ok(db):
    db.execute(transfos_sample)
    assert db.query("select check_istree(ARRAY[1, 2, 5])")[0][0]


def test_check_istree_ko_cycle(db):
    db.execute(transfos_sample)
    # insert transfo to make a cycle
    db.execute("insert into transfo (id, source, target) values (5, 4, 5)")
    assert not db.query("select check_istree(ARRAY[1, 2, 3, 4, 5])")[0][0]


def test_check_istree_ko_noconnex(db):
    db.execute(transfos_sample)
    # insert an isolated graph
    db.execute("insert into referential (id) values (6), (7)")
    db.execute("insert into transfo (id, source, target) values (6, 6, 7)")
    assert not db.query("select check_istree(ARRAY[1, 2, 3, 4, 6])")[0][0]


def test_platform_config_ok(db):
    db.execute(transfos_sample)
    db.execute(add_sensor_group)
    db.execute(add_transfo_trees)
    assert db.rowcount(add_platform_config) == 1


def test_platform_config_ko(db):
    db.execute(transfos_sample)
    db.execute(add_sensor_group)
    db.execute(add_transfo_trees)
    with pytest.raises(psycopg2.IntegrityError):
        db.execute('''
            insert into platform_config (id, platform, transfo_trees)
            values (1, 1, ARRAY[1, 2])
        ''')


def test_platform_config_ko2(db):
    db.execute(transfos_sample)
    db.execute(add_sensor_group)
    db.execute(add_transfo_trees)
    with pytest.raises(psycopg2.IntegrityError):
        db.execute('''
            insert into platform_config (id, platform, transfo_trees)
            values (1, 1, ARRAY[1, 2, 8])
        ''')


def test_check_transfotree_istree_empty_ok(db):
    db.execute(transfos_sample)
    db.execute(add_sensor_group)
    db.execute(add_transfo_trees)
    assert db.query("select check_transfotree_istree(ARRAY[1, 2, 3])")[0][0]


def test_check_transfotree_istree_empty_ko(db):
    db.execute(transfos_sample)
    db.execute(add_sensor_group)
    db.execute(add_transfo_trees)
    assert not db.query("select check_transfotree_istree(ARRAY[2, 1])")[0][0]


# FIXME activate when delete triggers will be ready
# def test_delete_transfo_cascade(db):
#     '''deleting a transfo should propagate deletion of related platform_config
#     and transfo_trees'''
#     db.execute(transfos_sample)
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

def test_dijkstra_function(db):
    db.execute(transfos_sample)
    db.execute(add_sensor_group)
    db.execute(add_transfo_trees)
    db.execute(add_platform_config)
    assert db.query("select dijkstra(1, 1, 5)")[0][0] == [1, 4, 5]
    assert db.query("select dijkstra(1, 3, 8)")[0][0] == [3, 4, 5, 8]
    assert db.query("select dijkstra(1, 1, 1)")[0][0] == [1]


def test_dijkstra_function_exception(db):
    db.execute(transfos_sample)
    db.execute(add_sensor_group)
    db.execute(add_transfo_trees)
    db.execute(add_platform_config)
    with pytest.raises(Exception):
        db.query("select dijkstra(1, 1, 55)")
    with pytest.raises(Exception):
        db.query("select dijkstra(1, 55, 1)")
