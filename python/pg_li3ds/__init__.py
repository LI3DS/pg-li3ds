# -*- coding: utf-8 -*-

import plpy
import json

__version__ = '0.1.dev0'


func_names = {
    'affine_mat4x3': 'PC_Affine',
    'affine_quat': 'PC_Affine',
    'spherical_to_cartesian': 'PC_SphericalToCartesian',
}


def dim_name(dim):
    ''' Return the dimension sign and name. '-' is returned when dim is negative and ''
        when dim is positive.
    '''
    neg = ''
    if dim[0] == '-':
        neg = '-'
        dim = dim[1:]
    return neg, dim


def append_dim_select(dim, select):
    ''' Append the PC_Get fonction call string for "dim" to "select".
    '''
    neg, dim = dim_name(dim)
    select.append('{}PC_Get(point, \'{}\') {}'.format(neg, dim, plpy.quote_ident(dim)))


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
            where pc_patchmin({column}, 'time') < {time:f} and
                  pc_patchmax({column}, 'time') > {time:f}
        ) select %s from patch
        ''' % select).format(schema=schema, table=table, column=column, time=time)
    plpy.debug(q)
    rv = plpy.execute(q)
    if len(rv) == 0:
        plpy.warning('no parameters for the provided time')
        return None
    if len(rv) != 1:
        plpy.error('multiple rows returned from time interpolation')
    values = rv[0]

    for key, param in params.items():
        if isinstance(param, list):
            for i, dim in enumerate(param):
                val = values[dim_name(dim)[1]]
                param[i] = val
        else:
            dim = param
            val = values[dim_name(dim)[1]]
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
    plpy.debug(q)
    plan = plpy.prepare(q, ['numeric'] * len(args_val))
    rv = plpy.execute(plan, args_val)
    if len(rv) != 1:
        plpy.error('unexpected returned value from {}'.format(q))
    result = rv[0]['r']
    return result


def _transform_box4d(box4d, func_name, func_sign, params):
    ''' Transform the box4d, using func_name, func_sign and params.
    '''
    return _transform(box4d, 'LIBOX4D', func_name, func_sign, params)


def _transform_patch(patch, func_name, func_sign, params):
    ''' Transform the patch, using func_name, func_sign and params.
    '''
    return _transform(patch, 'PCPATCH', func_name, func_sign, params)


def transform_box4d(box4d, transfoid, time):
    ''' Transform the box4d, using transfoid and time. time is ignored if the transform
        is static.
    '''
    transfo = get_transform(transfoid, time)
    if not transfo:
        return None
    name, params, func_name, func_sign = transfo
    plpy.log('apply transfo "{}" (function: "{}") to box4d'.format(name, func_name))
    return _transform_box4d(box4d, func_name, func_sign, params)


def transform_patch(patch, transfoid, time):
    ''' Transform the patch, using transfoid and time. time is ignored if the transform
        is static.
    '''
    transfo = get_transform(transfoid, time)
    if not transfo:
        return None
    name, params, func_name, func_sign = transfo
    plpy.log('apply transfo "{}" (function: "{}") to patch'.format(name, func_name))
    return _transform_patch(patch, func_name, func_sign, params)
