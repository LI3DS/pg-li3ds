-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION li3ds" to load this file. \quit


create or replace function check_timezone_name(timezone varchar)
returns boolean as $$
    declare rec record;
    begin
    select count(*) as cnt
    from pg_timezone_names where name = timezone into rec;
    return rec.cnt::int = 1;
    end;
$$ language plpgsql;

/*
Tables for metadata
*/
create table project(
    id serial primary key
    , name varchar unique not null
    , timezone varchar check (check_timezone_name(timezone))
    , extent geometry(polygon, 4326)
);

create type sensor_type as enum (
    'group',
    'camera',
    'lidar',
    'imu',
    'ins',
    'gnss',
    'odometer'
);

create table platform(
    id serial primary key
    , name varchar unique not null
    , description varchar
    , start_time timestamptz
    , end_time timestamptz
);

create table sensor(
    id serial primary key
    , name varchar unique not null
    , description varchar
    , serial_number varchar not null
    , short_name varchar -- FIXME brand_model_serial_number[:-3]
    , brand varchar
    , model varchar
    , type sensor_type not null
    , specifications jsonb
);

create table referential(
    id serial primary key
    , name varchar not null
    , description varchar
    , root boolean
    , srid int
    , sensor int references sensor(id)
    , constraint uniqreferential unique(name, sensor)
);

create table session(
    id serial primary key
    , name varchar unique not null
    , description varchar
    , start_time timestamptz -- computed
    , end_time timestamptz -- computed
    , project int references project(id) on delete cascade not null
    , platform int references platform(id) on delete cascade not null
);

create table datasource(
    id serial primary key
    , uri varchar
    , session int references session(id) on delete cascade not null
    , referential int references referential(id) on delete cascade not null
    , constraint uniqdatasource unique(uri, session, referential)
);

create table processing(
    id serial primary key
    , launched timestamptz
    , description varchar
    , tool varchar
    , source int references datasource(id) on delete cascade not null
    , target int references datasource(id) on delete cascade not null
);

create table posdatasource(
    id serial primary key
    , uri varchar
    , version int
    , session int references session(id) on delete cascade not null
    , sensor int references sensor(id) on delete cascade not null
    , constraint uniqposdatasource unique(uri, version, session, sensor)
);

create table posprocessing(
    id serial primary key
    , launched timestamptz
    , description varchar
    , tool varchar
    , source int references posdatasource(id) on delete cascade not null
    , target int references posdatasource(id) on delete cascade not null
);

create table transfo_type(
    id serial primary key
    , name varchar unique not null
    , func_signature varchar[]
    , description varchar
);

-- add constraint on transformation insertion
create or replace function check_transfo_args(parameters jsonb, transfo_type int)
returns boolean as $$
    declare
        rec record;
    begin
        select func_signature
        from li3ds.transfo_type t
        where t.id = transfo_type
        into rec;
        return parameters ?& rec.func_signature;
    end;
$$ language plpgsql;

create table transfo(
    id serial primary key
    , name varchar not null
    , description varchar
    , tdate timestamptz default now()
    , validity_start timestamptz default '-infinity'
    , validity_end timestamptz default 'infinity'
    , parameters jsonb check (check_transfo_args(parameters, transfo_type))
    , source int references referential(id) not null
    , target int references referential(id) not null
    , transfo_type int references transfo_type(id)
);

/*
-- check that treeview is a spanning tree
-- * acyclic graph = n vertex, n-1 edges
-- * connex
*/
create or replace function check_istree(transfos integer[])
returns boolean as $CODE$
    from collections import defaultdict, deque
    from itertools import chain
    import json

    success = True
    graph = {}

    # check connectivity
    # don't need sign
    # getting sources and targets for each transformation
    vals = '('+'),('.join([str(i)+','+str(r) for i, r in enumerate(transfos)])+')'
    rv = plpy.execute(
        """
        select id, source, target
        from (values {}) as v
        join li3ds.transfo t on v.column2 = t.id
        order by v.column1
        """.format(vals)
    )
    # fill the graph for later use
    for tra in rv:
        graph[tra['id']] = (tra['source'], tra['target'])

    # check connexity
    neighbors = defaultdict(set)
    # store referentials (nodes)
    nodes = set(chain.from_iterable(graph.values()))

    # graph must be acyclic
    if len(graph) >= len(nodes):
        plpy.warning('circular graph nodes: {}, egdes: {}'
                     .format(len(nodes), len(graph)))
        success = False

    for tra, refs in graph.items():
        neighbors[refs[0]].update({refs[1]})
        # non oriented graph
        neighbors[refs[1]].update({refs[0]})


    visited_nodes = {}
    start_node = list(nodes)[0]
    queue = deque()
    queue.append(start_node)
    visited_nodes[start_node] = True

    while queue:
        node = queue.popleft()
        for child in neighbors[node]:
            if child not in visited_nodes:
                visited_nodes[child] = True
                queue.append(child)

    diff = len(visited_nodes) - len(nodes)
    if diff:
        success = False
        plpy.warning(
            'disconnected graph, visited nodes {}, total {}'
            .format(len(visited_nodes), len(nodes))
        )

    return success
$CODE$ language plpython3u;

/*
Aggregates multi-dimensionnal array using array_cat
*/
create aggregate array_aggmult (int[])  (
    SFUNC     = array_cat,
    STYPE     = int[],
    INITCOND  = '{}'
);

/*
Aggregates transfos_trees and check if it's a spanning tree
*/
create or replace function check_transfotree_istree(transfo_trees integer[])
returns boolean as
$$
declare res boolean;
declare cnt integer;
declare inter integer[];
begin
    select count(*) into cnt
    from li3ds.transfo_tree where id = ANY($1);
    if cnt = 0 then
        raise notice 'no transfo_given';
        return true;
    end if;
    select li3ds.check_istree(array_aggmult(transfos)) into res
    from li3ds.transfo_tree where id = ANY($1);
    return res;
end;
$$ language plpgsql;

-- add constraint on transfo_tree insertion
create or replace function foreign_key_array(arr integer[], foreign_table regclass)
returns boolean as $$
declare
rec record;
begin
      execute format('
        with tmp as (
         select
              t.*, val
          from
              unnest(''%s''::integer[]) as val
          left join %s t on t.id = val::int
          where t.id is null
      )
      select count(*) as cnt, string_agg(val::text, '','') as arr
      from tmp', arr, foreign_table) into rec;

      if rec.cnt::int != 0 then
          raise warning 'following foreign keys don''t exists: %', rec.arr;
          return false;
      else
          return true;
      end if;
  end;
$$ language plpgsql;


create table transfo_tree(
    id serial primary key
    , name varchar not null
    , description varchar
    , isdefault boolean
    , owner varchar
    , sensor_connections boolean default false
    , transfos integer[]
	check (
        foreign_key_array(transfos, 'li3ds.transfo')
        and (sensor_connections or check_istree(transfos))
    )
);

create table platform_config(
    id serial primary key
    , name varchar unique not null
    , description varchar
    , owner varchar
    , platform integer references platform(id) not null
    , transfo_trees integer[]
    check (
        foreign_key_array(transfo_trees, 'li3ds.transfo_tree')
        and check_transfotree_istree(transfo_trees)
    )
);

/*
Function that creates a project inside a specific schema.
Returns the project id.
*/
create or replace function create_project(project_name varchar, timezone varchar DEFAULT 'Europe/Paris', extent varchar DEFAULT NULL)
returns integer as
$$
declare proj_id int;
begin

    execute format('
        insert into li3ds.project (name, timezone, extent)
	    values (%L, %L, %L::geometry) returning id', $1, $2, $3)
        into proj_id;

    execute format('create schema %I', lower($1));

    execute format('create table %I.image(
          id bigserial primary key
          , filename varchar
          , exif jsonb
          , etime timestamptz
          , datasource bigint references li3ds.datasource(id) on delete cascade
        );', lower($1));

    RETURN proj_id;
END;
$$ language plpgsql;

/*
Delete dataset schema
*/
create or replace function delete_project(project_name varchar)
returns void as
$$
begin
    execute format('delete from li3ds.project where name = %L', $1);
    execute format('drop schema if exists %I cascade', lower($1));
    RETURN;
END;
$$ language plpgsql;


create or replace function dijkstra(platform_config_id integer,
    source integer, target integer)
returns integer[] as
$CODE$
    from heapq import heappop, heappush

    # get all transformations involved in the transfo tree list
    transfo_list = plpy.execute(
        """
        select array_aggmult(tt.transfos) as trf
        from li3ds.platform_config pf
        join li3ds.transfo_tree tt on tt.id = ANY(pf.transfo_trees)
        where pf.id = {}
        """.format(platform_config_id)
    )[0]['trf']

    # graph defined by adjacency list of edges (more usefull than nodes for us)
    # graph = {edge: [(weigh, edge1), (weight, edge2)...], ...}
    adj_list = plpy.execute(
        """
        select t.id, array_agg(ta.id) as adj_list
        from li3ds.transfo t,
        unnest(array[t.source, t.target]) as ref
        join li3ds.transfo ta on array[ta.id] <@ array[{}]
        and (ref = ta.source or ref = ta.target)
        where t.id != ta.id
        group by t.id;
        """.format(','.join(map(str, transfo_list)))
    )
    # contructs graph
    graph = {
        column['id']: [(1, idt) for idt in column['adj_list']]
        for column in adj_list
    }

    if source not in graph:
        raise Exception("No transformation with id {}".format(source))
    if target not in graph:
        raise Exception("No transformation with id {}".format(target))

    M = set()
    d = {source: 0}
    p = {}
    suivants = [(0, source)]

    while suivants != []:

        dx, x = heappop(suivants)
        if x in M:
            continue

        M.add(x)

        for w, y in graph[x]:
            if y in M:
                continue
            dy = dx + w
            if y not in d or d[y] > dy:
                d[y] = dy
                heappush(suivants, (dy, y))
                p[y] = x

    shortest_path = [target]
    x = target
    while x != source:
        x = p[x]
        shortest_path.insert(0, x)

    return shortest_path
$CODE$ language plpython3u;
