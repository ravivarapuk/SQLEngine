import os
import sys
import csv
import numpy as np
import re
import pprint
import itertools

DB_DIR = "../files/"
META_FILE = "../files/metadata.txt"
AGGREGATE = ["max", "min", "sum", "avg", "count", "distinct"]
OPS = ["<=", ">=", "=", "<>", "<", ">"]
LITERAL = "<literal>"

schema = {}


def _error_if(x, s):
    if x:
        # assert not x, s
        print("ERROR : {}".format(s))
        exit(-1)


def _isint(s):
    try:
        _ = int(s)
        return True
    except:
        return False


def _get_relate_op(cond):

    if "<=" in cond:
        op = "<="
    elif ">=" in cond:
        op = ">="
    elif "<>" in cond:
        op = "<>"
    elif ">" in cond:
        op = ">"
    elif "<" in cond:
        op = "<"
    elif "=" in cond:
        op = "="
    else:
        _error_if(True, "invalid condition : '{}'".format(cond))

    _error_if(cond.count(op) != 1, "invalid condition : '{}'".format(cond))
    l, r = cond.split(op)
    l = l.strip()
    r = r.strip()
    return op, l, r


def _init_metadata():
    with open(META_FILE, "r") as f:
        contents = f.readlines()
    contents = [t.strip() for t in contents if t.strip()]

    table_name = None
    for t in contents:
        t = t.lower()
        if t == "<begin_table>":
            attrs, table_name = [], None
        elif t == "<end_table>":
            pass
        elif not table_name:
            table_name, schema[t] = t, []
        else:
            schema[table_name].append(t)


def _load_table(fname):
    # d1 = list(csv.reader(open(fname, "r")
    # return list(map(lambda x : list(map(int, x)), l1))
    return np.genfromtxt(fname, dtype=int, delimiter=',')


def _get_op_tbl(tdict):
    pprint.pprint(tdict)
    alias2tb = tdict['alias2tb']
    inter_cols = tdict['inter_cols']
    tables = tdict['tables']
    conditions = tdict['conditions']
    cond_op = tdict['cond_op']
    proj_cols = tdict['proj_cols']

    # load all tables and retain only necessary columns
    # also decide the indexes of intermediate table columns
    colidx = {}
    cnt = 0
    all_tables = []
    for t in tables:
        lt = _load_table(os.path.join(DB_DIR, "{}.csv".format(alias2tb[t])))

        idxs = [schema[alias2tb[t]].index(cnmae) for cname in inter_cols[t]]
        lt = lt[:, idxs]
        all_tables.append(lt.tolist())

        colidx[t] = {cname: cnt+i for i, cname in enumerate(inter_cols[t])}
        cnt += len(inter_cols[t])

    # cartesian product of all tables
    inter_tbl = [[i for tup in r for i in list(tup)] for r in itertools.product(*all_tables)]
    inter_tbl = np.array(inter_tbl)

    # check for conditions and get reduced table
    if len(conditions):
        totake = np.ones((inter_tbl.shape[0], len(conditions)), dtype=bool)

        for idx, (op, left, right) in enumerate(conditions):
            cols = []
            for tname, cname in [left, right]:
                if tname == LITERAL:
                    cols.append(np.full((inter_tbl.shape[0]), int(cname)))
                else:
                    cols.append(inter_tbl[:, colidx[tname][cname]])

            if op == "<=":
                totake[:, idx] = (cols[0] <= cols[1])
            if op == ">=":
                totake[:, idx] = (cols[0] >= cols[1])
            if op == "<>":
                totake[:, idx] = (cols[0] != cols[1])
            if op == "<":
                totake[:, idx] = (cols[0] < cols[1])
            if op == ">":
                totake[:, idx] = (cols[0] > cols[1])
            if op == "=":
                totake[:, idx] = (cols[0] == cols[1])
        if cond_op == " or ":
            final_take = (totake[:, 0] | totake[:, 1])
        elif cond_op == " and ":
            final_take = (totake[:, 0] & totake[:, 1])
        else:
            final_take = totake[:, 0]
        inter_tbl = inter_tbl[final_take]

    select_idxs = [colidx[tn][cn] for tn, cn, aggr in proj_cols]
    inter_tbl = inter_tbl[:, select_idxs]

    # process for aggregate functions
    if proj_cols[0][2]:
        out_table = []
        disti = False
        for idx, (tn, cn, aggr) in enumerate(proj_cols):
            col = inter_tbl[:, idx]
            if aggr == "min":
                out_table.append(min(col))
            elif aggr == "max":
                out_table.append(max(col))
            elif aggr == "sum":
                out_table.append(sum(col))
            elif aggr == "avg":
                out_table.append(sum(col)/col.shape[0])
            elif aggr == "count":
                out_table.append(col.shape[0])
            elif aggr == "distinct":
                seen = set()
                out_table = [x for x in col.tolist() if not (x in seen or seen.add(x))]
                disti = True
            else:
                _error_if(True, "invalid aggregate")
        out_table = np.array([out_table])
        if disti:
            out_table = np.array(out_table).T
        out_header = ["{}({}.{})".format(aggr, tn, cn) for tn, cn, aggr in proj_cols]
    else:
        out_table = inter_tbl
        out_header = ["{}.{}". format(tn, cn) for tn, cn, aggr in proj_cols]
    return out_header, out_table.tolist()


def _break_query(q):
    # to check the structure of select, from and where
    if type(q) is str:
        toks = q.lower().split()
        if toks[0] != "select":
            _error_if(True, "Only select is allowed")
        select_idx = [idx for idx, t in enumerate(toks) if t == "select"]
        from_idx = [idx for idx, t in enumerate(toks) if t == "from"]
        where_idx = [idx for idx, t in enumerate(toks) if t == "where"]

        _error_if((len(select_idx) != 1) or (len(from_idx) != 1) or (len(where_idx) != 1), "invalid query")
        select_idx, from_idx = select_idx[0], from_idx[0]
        where_idx = where_idx[0] if len(where_idx) == 1 else None
        _error_if(from_idx <= select_idx, "invalid query")
        if where_idx:
            _error_if(where_idx <= from_idx, "invalid query")

        raw_cols = toks[select_idx + 1:from_idx]

        if where_idx:
            raw_tables = toks[from_idx + 1:where_idx]
            raw_condition = toks[where_idx + 1:]
        else:
            raw_tables = toks[from_idx + 1:]
            raw_condition = []

        _error_if(len(raw_tables) == 0, "no tables after 'from'")
        _error_if(where_idx != None and len(raw_condition) == 0, "no conditions after 'where'")
        # ----------------------------------------------
        return raw_tables, raw_cols, raw_condition


def _parse_tables(raw_tables):
    # all joined tables

    raw_tables = " ".join(raw_tables).split(",")
    tables = []
    alias2tb = {}
    for rt in raw_tables:
        t = rt.split()
        _error_if(not(len(t) == 1 or (len(t) == 3 and t[1] == "as")), "invalid table specification '{}'".format(rt))
        if len(t) == 1:
            tb_name, tb_alias = t[0], t[0]
        else:
            tb_name, _, tb_alias = t

        _error_if(tb_name not in schema.keys(), "no table name '{}'".format(tb_name))
        _error_if(tb_alias in alias2tb.keys(), "no unique table/alias '{}'".format(tb_name))

        tables.append(tb_alias)
        alias2tb[tb_alias] = tb_name

    return tables, alias2tb


def _parse_porj_cols(raw_cols, tables, alias2tb):
    # projection columns: cols to output

    raw_cols = "".join(raw_cols).split(",")
    proj_cols = []
    for rc in raw_cols:
        # match for aggregate function
        reg_match = re.match("(.+)\((.+)\)", rc)
        if reg_match:
            aggr, rc = reg_match.groups()
        else:
            aggr = None

        # either one of these two : col or table.col
        _error_if("." in rc and len(rc.split(".")) != 2, "invalid column name '{}'".format(rc))

        # get table name and column name
        tname = None
        if "." in rc:
            tname, cname = rc.split(".")
            _error_if(tname not in alias2tb.keys(), "unknown field : '{}'".format(rc))
        else:
            cname = rc
            if cname != "*":
                tname = [t for t in tables if cname in schema[alias2tb[t]]]
                _error_if(len(tname) > 1, "not unique field : '{}'".format(rc))
                _error_if(len(tname) == 0, "unknown field : '{}'".format(rc))
                tname = tname[0]
                # add all columns if *
                if cname == "*":
                    _error_if(aggr != None, "can't use aggregate '{}'".format(aggr))
                    if tname != None:
                        proj_cols.extend([(tname, c, aggr) for c in schema[alias2tb[tname]]])
                    else:
                        for t in tables:
                            proj_cols.extend([(t, c, aggr) for c in schema[alias2tb[t]]])
                else:
                    _error_if(cname not in schema[alias2tb[tname]], "unknown field : '{}'".format(rc))
                    proj_cols.append((tname, cname, aggr))

            # either all columns without aggregate or all columns with aggregate
            s = [a for t, c, a in proj_cols]
            _error_if(all(s) ^ any(s), "aggregated and non-aggregated columns are not allowed simultaneously")
            _error_if(any([(a == "distinct") for a in s]) and len(s) != 1, "distinct can only be used alone")

            return proj_cols


def parse_conditions(raw_condition, tables, alias2tb):
    # parse conditions

    conditions = []
    cond_op = None
    if raw_condition:
        raw_condition = " ".join(raw_condition)

        if " or " in raw_condition:
            cond_op = " or "
        elif " and " in raw_condition:
            cond_op = " and "

        if cond_op:
            raw_condition = raw_condition.split(cond_op)
        else:
            raw_condition = [raw_condition]

        for cond in raw_condition:
            relate_op, left, right = _get_relate_op(cond)
            parsed_cond = [relate_op]
            for idx, rc in enumerate([left, right]):
                if _isint(rc):
                    parsed_cond.append((LITERAL, rc))
                    continue

                if "." in rc:
                    tname, cname = rc.split(".")
                else:
                    cname = rc
                    tname = [t for t in tables if rc in schema[alias2tb[t]]]
                    _error_if(len(tname) > 1, "not unique field : '{}'".format(rc))
                    _error_if(len(tname) == 0, "unknown field : '{}'".format(rc))
                    tname = tname[0]
                _error_if((tname not in alias2tb.keys()) or (cname not in schema[alias2tb[tname]]),
                    "unknown field : '{}'".format(rc))
                parsed_cond.append((tname, cname))
            conditions.append(parsed_cond)

    return conditions, cond_op


def _parse_query(q):
    # parse the query

    # break query
    raw_tables, raw_cols, raw_condition = _break_query(q)
