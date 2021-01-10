from argparse import ArgumentParser
import os
import yaml
from attrdict import AttrDict

def qstr(queries: list) -> str:
    return '\n'.join(map(lambda x: f'{x};', queries))

def core(conf_path, workdir):

    q = []
    s = []

    db_sel = 'database'
    tbl_sel = 'table'

    conf = AttrDict(yaml.safe_load(open(conf_path)))

    test_name = conf.name
    db_actual = f'{test_name}_actual'
    db_expected = f'{test_name}_expected'

    # create databases.
    q += [
        '-- create datbases',
        f'DROP DATABASE IF EXISTS {db_actual} CASCADE',
        f'DROP DATABASE IF EXISTS {db_expected} CASCADE',
        f'CREATE DATABASE {db_actual}',
        f'CREATE DATABASE {db_expected}',
    ]

    # create tables.
    q += ['-- create tables']
    for p in conf.ddl:
        path = f'{workdir}/{p}'
        ddl = open(path).read()
        q += [
            f'SET hivevar:database={db_actual}',
            ddl,
            f'SET hivevar:database={db_expected}',
            ddl,
        ]

    # execute setup queries.
    q += ['-- execute setup queries']
    q += conf.setup

    # insert testdata into the tables.
    a = {}
    e = {}
    for t in conf.testdata:
        for k, v in t.get('actual', {}).items():
            if isinstance(v, tuple):
                a[k] = a.get(k, ()) + v
            else:
                a[k] = a.get(k, ()) + (v,)
        for k, v in t.get('expected', {}).items():
            if isinstance(v, tuple):
                e[k] = e.get(k, ()) + v
            else:
                e[k] = e.get(k, ()) + (v,)

    q += ['-- insert testdata into the tables']
    for k, v in a.items():
        vstr = ', '.join(map(lambda x: f'({x})', v))
        q += [
            f'INSERT OVERWRITE TABLE {db_actual}.{k} VALUES {vstr}'
        ]

    for k, v in e.items():
        vstr = ', '.join(map(lambda x: f'({x})', v))
        q += [
            f'INSERT OVERWRITE TABLE {db_expected}.{k} VALUES {vstr}'
        ]

    # execute dml.
    q += ['-- execute dml']
    for p in conf.dml:
        path = f'{workdir}/{p}'
        dml = open(path).read()
        q += [
            f'SET hivevar:database={db_actual}',
            f'SET hivevar:database_actual={db_actual}',
            f'SET hivevar:database_expected={db_expected}',
            dml,
        ]

    # validate output.
    q += ['-- validate output']
    for t in conf.validation:
        path = f'/user/hive/warehouse/{db_actual}.db/{t}/000000_0'
        q += [
            f'!grep true {path}',
        ]
        s += [
            f'grep true {path}',
            'if [ $? -ne 0 ]; then exit 1; fi',
        ]

    return test_name, q, s

def main():
    parser = ArgumentParser()
    parser.add_argument('conf_path')
    parser.add_argument('--workdir', '-w')
    parser.add_argument('--outdir', '-o')
    args = parser.parse_args()

    kwargs = dict(
        conf_path=args.conf_path,
        workdir=args.workdir if args.workdir else os.path.dirname(args.conf_path),
    )

    test_name, q, s = core(**kwargs)

    if args.outdir:
        q_path = f'{args.outdir}/{test_name}.q'
        sh_path = f'{args.outdir}/{test_name}.sh'
        f = open(q_path, 'w')
        f.write(qstr(q))
        f.close()
        f = open(sh_path, 'w')
        f.write('\n'.join(s))
        f.close()
    else:
        print(qstr(q))

if __name__ == '__main__':
    main()
