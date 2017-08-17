# -*- coding: utf-8 -*-

import plpy
import json

__version__ = '0.1.dev0'


func_names = {
    'affine_mat4x3': 'PC_Affine',
    'affine_quat': 'PC_Affine',
    'spherical_to_cartesian': 'PC_SphericalToCartesian',
}


def get_transform(transfoid):
    ''' Return a dict with keys "name", "params", "func_name", and "func_sign".
    '''
    q = '''
        select t.name as name, t.parameters as params,
               tt.name as func_name, tt.func_signature as func_sign
        from li3ds.transfo t
        join li3ds.transfo_type tt on t.transfo_type = tt.id
        where t.id = {:d}
        '''.format(transfoid)
    rv = plpy.execute(q)
    if len(rv) < 1:
        plpy.error('no transfo with id {:d}'.format(transfoid))
    transfo = rv[0]
    params = json.loads(transfo['params'])
    if params:
        params = params[0]  # assume the transform is static
    return transfo['name'], params, transfo['func_name'], transfo['func_sign']


def _transform(obj, type_, func_name, func_sign, params):
    if func_name not in func_names:
        plpy.error('function {} is unknown'.format(func_name))
    func_name = func_names[func_name]
    if isinstance(params, basestring):  # NOQA
        params = json.loads(params)
    args = [params[p] for p in func_sign if p != '_time']
    args_str = ''
    for arg in args:
        args_str += ', '
        if isinstance(arg, list):
            args_str += 'ARRAY'
        args_str += '{}'.format(arg)
    q = 'select {}(\'{}\'::{}{}) r'.format(func_name, obj, type_, args_str)
    rv = plpy.execute(q)
    if len(rv) != 1:
        plpy.error('unexpected returned value from {}'.format(q))
    result = rv[0]['r']
    return result


def _transform_box4d(box4d, func_name, func_sign, params):
    return _transform(box4d, 'LIBOX4D', func_name, func_sign, params)


def _transform_patch(patch, func_name, func_sign, params):
    return _transform(patch, 'PCPATCH', func_name, func_sign, params)


def transform_box4d(box4d, transfoid):
    transfo = get_transform(transfoid)
    name, params, func_name, func_sign = transfo
    plpy.log('apply transfo "{}" (function: "{}") to box4d'.format(name, func_name))
    return _transform_box4d(box4d, func_name, func_sign, params)


def transform_patch(patch, transfoid):
    transfo = get_transform(transfoid)
    name, params, func_name, func_sign = transfo
    plpy.log('apply transfo "{}" (function: "{}") to patch'.format(name, func_name))
    return _transform_patch(patch, func_name, func_sign, params)
