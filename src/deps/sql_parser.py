import os
import sys
import csv
import numpy as np
import re
import pprint
import itertools

DB_DIR = ""
META_FILE = ""
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