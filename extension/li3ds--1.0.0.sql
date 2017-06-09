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

create type datasource_type as enum (
    'image',
    'trajectory',
    'pointcloud'
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
    , brand varchar
    , model varchar
    , type sensor_type not null
    , specifications jsonb
);

create table referential(
    id serial primary key
    , name varchar not null
    , description varchar
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

create or replace function check_datasource_uri(uri text)
returns boolean as $$
    declare
        path_ text;
        scheme text;
        uri_split text[];
        path_split text[];
        exists_ boolean;
    begin
        uri_split := regexp_split_to_array(uri, ':');

        if array_length(uri_split, 1) <> 2 then
            return false;
        end if;

        scheme := uri_split[1]; path_ := uri_split[2];

        if scheme <> all (ARRAY['file', 'column']) then
            return false;
        end if;

        if scheme = 'column' then
            path_split := regexp_split_to_array(path_, '\.');

            if array_length(path_split, 1) <> 3 then
                return false;
            end if;

            exists_ := exists(
                select 1 from information_schema.columns where
                    table_schema=path_split[1] and
                    table_name=path_split[2] and
                    column_name=path_split[3]);

            if not exists_ then
                return false;
            end if;

        end if;

        return true;
    end;
$$ language plpgsql;

create table datasource(
    id serial primary key
    -- possible uri schemes are "file" and "column"
    , uri text not null constraint uri_scheme check (check_datasource_uri(uri))
    , type datasource_type not null
    , parameters jsonb
    , bounds double precision[6]  -- [xmin, ymin, zmin, xmax, ymax, zmax]
    , capture_start timestamptz
    , capture_end timestamptz
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

create table transfo_type(
    id serial primary key
    , name varchar unique not null
    , func_signature varchar[]
    , description varchar
);

-- add constraint on transformation insertion
create or replace function check_transfo_args(parameters jsonb, transfo_type_id int)
returns boolean as $$
    declare
        transfo_type record;
        signature varchar[];
        element record;
    begin
        if parameters is null then
            return true;
        end if;

        if jsonb_typeof(parameters) <> 'array' then
            return false;
        end if;

        select func_signature from li3ds.transfo_type t
            where t.id = transfo_type_id into transfo_type;
        if transfo_type is null then
            return false;
        end if;

        signature := transfo_type.func_signature;

        -- _time not mandatory is there's only one element is the parameters array
        if jsonb_array_length(parameters) < 2 then
            signature := array_remove(signature, '_time');
        end if;

        if jsonb_array_length(parameters) = 0 and array_length(signature, 1) >= 1 then
            return false;
        end if;

        for element in select jsonb_array_elements(parameters) json loop
            if not (element.json ?& signature) then
                return false;
            end if;
        end loop;

        return true;
    end;
$$ language plpgsql;

create or replace function check_pcpatch_column(schema_table_column varchar)
returns boolean as $$
    declare
      schema_table_column_array text[];
    begin
        if schema_table_column is null then
            return true;
        end if;
        schema_table_column_array := regexp_split_to_array(schema_table_column, E'\\.');
        if array_length(schema_table_column_array, 1) <> 3 then
            return false;
        end if;
        return exists(
            select 1 from information_schema.columns where
                table_schema=schema_table_column_array[1] and
                table_name=schema_table_column_array[2] and
                column_name=schema_table_column_array[3] and
                udt_name='pcpatch'
        );
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
    , parameters_column varchar check (check_pcpatch_column(parameters_column))
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
    , owner varchar
    , sensor integer references sensor(id)
    , transfos integer[]
	check (
        foreign_key_array(transfos, 'li3ds.transfo')
        and (sensor is NULL or check_istree(transfos))
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

    # graph defined by adjacency list of edges (more usefull than nodes for us)
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
