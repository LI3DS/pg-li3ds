# -*- coding: utf-8 -*-
from heapq import heappop, heappush
from collections import defaultdict, deque
from itertools import chain
import json

import plpy


__version__ = '0.1.dev0'


func_names = {
    'affine_mat4x3': 'PC_Affine',
    'affine_quat': 'PC_Affine',
    'affine_quat_inverse': 'PC_AffineInverse',
    'spherical_to_cartesian': 'PC_SphericalToCartesian',
    'projective_pinhole': 'PC_ProjectivePinhole',
    'projective_pinhole_inverse': 'PC_ProjectivePinholeInverse',
}


def isconnected(transfos, doubletransfo=False):
    """
    Check if transfos list corresponds to a connected graph
    """
    success = True
    edges = {}

    # check connectivity
    # getting sources and targets for each transformation
    tlist = ['{},{}'.format(i, r) for i, r in enumerate(transfos)]
    vals = '({})'.format('),('.join(tlist))
    rv = plpy.execute(
        """
        select id, source, target
        from (values {}) as v
        join li3ds.transfo t on v.column2 = t.id
        order by v.column1
        """.format(vals)
    )
    transfoset = set([(r['source'], r['target']) for r in rv])
    if not doubletransfo and len(transfoset) != len(rv):
        # multiple edges between source and target
        return False

    # fill the edges for later use
    for tra in rv:
        edges[tra['id']] = (tra['source'], tra['target'])

    # check connexity
    neighbors = defaultdict(set)
    # store referentials (nodes)
    nodes = set(chain.from_iterable(edges.values()))

    for tra, refs in edges.items():
        neighbors[refs[0]].update({refs[1]})
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


def dijkstra(config, source, target, stoptosensor=''):
    '''
    returns the transfo list needed to go from source referential to target
    referential
    '''

    # get all transformations involved in the transfo tree list
    transfo_list = plpy.execute(
        """
        select array_aggmult(tt.transfos) as trf
        from li3ds.platform_config pf
        join li3ds.transfo_tree tt on tt.id = ANY(pf.transfo_trees)
        where pf.id = {}
        """.format(config)
    )[0]['trf']

    transfo_list_coma_separated = ','.join(map(str, transfo_list))

    # list of adjacent nodes (referentials)
    # adj_list = [ref1: [ref7, ref1], ref2: [ref3]...]
    result = plpy.execute(
        """
        select
            r.id
            , array_agg(t.target)
              filter (where t.target is not NULL) as adj_list
              -- we keep a NULL column instead of an array
              -- with a null value inside
        from li3ds.referential r
        left join li3ds.transfo t
            -- we only keep direct transformations
            on t.source = r.id
            and array[t.id] <@ array[{}]::integer[]
        group by r.id
        """.format(transfo_list_coma_separated)
    )
    # build graph
    # graph = {ref1: [(1, ref7), (1, ref3)...], ...}
    graph = {}
    for column in result:
        if column['adj_list'] is not None:
            graph[column['id']] = [(1, idt) for idt in column['adj_list']]
        else:
            graph[column['id']] = []

    if source not in graph:
        raise Exception("No referential with id {}".format(source))
    if target not in graph:
        raise Exception("No referential with id {}".format(target))

    M = set()
    d = {source: 0}
    p = {}
    next_nodes = [(0, source)]

    while next_nodes:

        dx, x = heappop(next_nodes)
        if x in M:
            continue

        M.add(x)

        for w, y in graph[x]:
            if y in M:
                continue
            dy = dx + w
            if y not in d or d[y] > dy:
                d[y] = dy
                heappush(next_nodes, (dy, y))
                p[y] = x

    shortest_path = [target]
    x = target
    while x != source:
        try:
            x = p[x]
        except KeyError:
            plpy.notice("No path from ref:{} to ref:{} with config {}"
                        .format(source, target, config))
            return []
        shortest_path.insert(0, x)

    if stoptosensor:
        # if a sensor type was requested we want to return
        # the first referential matching that type
        for ref in shortest_path:
            found = plpy.execute("""
                select r.id, s.type from referential r
                join sensor s on r.sensor = s.id
                where r.id = {}""".format(ref))
            if found[0]['type'] == stoptosensor:
                return [found[0]['id']]
        raise Exception(
            "No referential in path with type {}".format(stoptosensor))

    # we have referentials now we need all transformations
    # assembling refs by pair
    ref_pair = [
        shortest_path[i:i + 2]
        for i in range(0, len(shortest_path) - 1)]

    transfos = []
    for ref_source, ref_target in ref_pair:
        transfos.append(plpy.execute(
            """
            select id
            from li3ds.transfo
            where source = {} and target = {}
            and array[id] <@ array[{}]::integer[]
            """.format(ref_source, ref_target, transfo_list_coma_separated))[0]['id'])

    return transfos


def append_dim_select(dim, select):
    ''' Append the PC_Get fonction call string for "dim" to "select".
    '''
    select.append('PC_Get(point, \'{}\') {}'.format(dim, plpy.quote_ident(dim)))


def get_dyn_transfo_params(params_column, params, time):
    ''' Return the dynamic transfo parameters.
    '''
    schema, table, column = tuple(map(plpy.quote_ident, params_column.split('.')))
    params = params[0]

    select = []
    for param in params.values():
        if isinstance(param, list):
            for dim in param:
                append_dim_select(dim, select)
        else:
            dim = param
            append_dim_select(dim, select)
    select = ', '.join(select)

    q = ('''
        with patch as (
            select pc_interpolate({column}, 'time', {time:f}, true) point
            from {schema}.{table}
            where pc_patchmin({column}, 'time') <= {time:f} and
                  pc_patchmax({column}, 'time') >  {time:f}
        ) select %s from patch
        ''' % select).format(schema=schema, table=table, column=column, time=time)
    plpy.debug(q)
    rv = plpy.execute(q)
    if len(rv) == 0:
        plpy.warning('no parameters for the provided time ({:f})'.format(time))
        return None
    if len(rv) != 1:
        plpy.error('multiple rows returned from time interpolation')
    values = rv[0]

    for key, param in params.items():
        if isinstance(param, list):
            for i, dim in enumerate(param):
                val = values[dim]
                param[i] = val
        else:
            dim = param
            val = values[dim]
            params[key] = val

    return params


def get_transform(transfoid, time):
    ''' Return information about the transfo whose id is transfoid. A dict with keys "name",
        "params", "func_name", and "func_sign".
    '''
    q = '''
        select t.name as name,
               t.parameters_column as params_column, t.parameters as params,
               tt.name as func_name, tt.func_signature as func_sign
        from li3ds.transfo t
        join li3ds.transfo_type tt on t.transfo_type = tt.id
        where t.id = {:d}
        '''.format(transfoid)
    plpy.debug(q)
    rv = plpy.execute(q)
    if len(rv) < 1:
        plpy.error('no transfo with id {:d}'.format(transfoid))
    transfo = rv[0]
    params_column = transfo['params_column']
    params = json.loads(transfo['params'])
    if params_column:
        # dynamic transfo
        if not time:
            plpy.error('no time value provided for dynamic transfo "{}"'.format(transfo['name']))
        params = get_dyn_transfo_params(params_column, params, time)
        if params is None:
            return None
    elif params:
        params = params[0]  # assume the transform is static
    return transfo['name'], params, transfo['func_name'], transfo['func_sign']


def args_to_array_string(args):
    ''' Return args wrapped into ARRAY[]'s.
    '''
    args_str = ''
    args_val = []
    idx = 1
    for arg in args:
        args_str += ', '
        if isinstance(arg, list):
            str_ = 'ARRAY[{}]'.format(
                 ','.join('${}'.format(idx + i) for i in range(len(arg))))
            args_str += str_
            args_val.extend(arg)
            idx += len(arg)
        else:
            args_str += '$1'
            args_val.append(arg)
            idx += 1
    return args_str, args_val


def _transform(obj, type_, func_name, func_sign, params):
    ''' Transform obj, whose type is type_, using func_name, func_sign and params.
    '''
    if func_name not in func_names:
        plpy.error('function {} is unknown'.format(func_name))
    func_name = func_names[func_name]
    if isinstance(params, basestring):  # NOQA
        params = json.loads(params)
    args = [params[p] for p in func_sign if p != '_time']
    args_str, args_val = args_to_array_string(args)
    q = 'select {}(\'{}\'::{}{}) r'.format(func_name, obj, type_, args_str)
    plpy.debug(q, args_val)
    plan = plpy.prepare(q, ['numeric'] * len(args_val))
    rv = plpy.execute(plan, args_val)
    if len(rv) != 1:
        plpy.error('unexpected number of rows ({}) returned from {}'.format(len(rv), q))
    result = rv[0].get('r')
    if result is None:
        plpy.error('unexpected value None returned from {}'.format(q))
    return result


def _transform_box4d(box4d, func_name, func_sign, params):
    ''' Transform the box4d, using func_name, func_sign and params.
    '''
    return _transform(box4d, 'LIBOX4D', func_name, func_sign, params)


def transform_box4d_one(box4d, transfoid, time):
    ''' Transform the box4d, using transfoid and time. time is ignored if the transform
        is static.
    '''
    transfo = get_transform(transfoid, time)
    if not transfo:
        return None
    name, params, func_name, func_sign = transfo
    plpy.log('apply transfo "{}" (function: "{}") to box4d'.format(name, func_name))
    return _transform_box4d(box4d, func_name, func_sign, params)


def transform_box4d_list(box4d, transfoids, time):
    ''' Transform the box4d, using all the transforms in the transfoids list. '''
    for transfoid in transfoids:
        box4d = transform_box4d_one(box4d, transfoid, time)
        if not box4d:
            break
    return box4d


def transform_box4d_config(box4d, config, source, target, time):
    ''' Apply the transform path from "source" to "target" for the provided "config".
    '''
    transforms = dijkstra(config, source, target)
    return transform_box4d_list(box4d, transforms, time)


def _transform_point(point, func_name, func_sign, params):
    ''' Transform the point, using func_name, func_sign and params.
    '''
    point_str = ' '.join(map(str, point))
    box4d = 'BOX4D({point_str},{point_str})'.format(point_str=point_str)
    box4d_out = _transform_box4d(box4d, func_name, func_sign, params)
    point_out = list(map(float, box4d_out[6:-1].split(',')[0].split(' ')))
    return point_out


def transform_point_one(point, transfoid, time):
    ''' Transform the point, using transfoid and time. time is ignored if the transform
        is static.
    '''
    transfo = get_transform(transfoid, time)
    if not transfo:
        return None
    name, params, func_name, func_sign = transfo
    plpy.log('apply transfo "{}" (function: "{}") to point'.format(name, func_name))
    return _transform_point(point, func_name, func_sign, params)


def transform_point_list(point, transfoids, time):
    ''' Transform the point, using all the transforms in the transfoids list. '''
    for transfoid in transfoids:
        point = transform_point_one(point, transfoid, time)
        if not point:
            break
    return point


def transform_point_config(point, config, source, target, time):
    ''' Apply the transform path from "source" to "target" for the provided "config".
    '''
    transforms = dijkstra(config, source, target)
    return transform_point_list(point, transforms, time)


def _transform_patch(patch, func_name, func_sign, params):
    ''' Transform the patch, using func_name, func_sign and params.
    '''
    return _transform(patch, 'PCPATCH', func_name, func_sign, params)


def transform_patch_one(patch, transfoid, time):
    ''' Transform the patch, using transfoid and time. time is ignored if the transform
        is static.
    '''
    transfo = get_transform(transfoid, time)
    if not transfo:
        return None
    name, params, func_name, func_sign = transfo
    plpy.log('apply transfo "{}" (function: "{}") to patch'.format(name, func_name))
    return _transform_patch(patch, func_name, func_sign, params)


def transform_patch_list(patch, transfoids, time):
    ''' Transform the patch, using all the transforms in the transfoids list. '''
    for transfoid in transfoids:
        patch = transform_patch_one(patch, transfoid, time)
        if not patch:
            break
    return patch


def transform_patch_config(patch, config, source, target, time):
    ''' Apply the transform path from "source" to "target" for the provided "config".
    '''
    transforms = dijkstra(config, source, target)
    return transform_patch_list(patch, transforms, time)
