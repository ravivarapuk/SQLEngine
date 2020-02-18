import sys
from src.deps.sql_parser import _init_metadata, _parse_query, _get_op_tbl, _print_table

# directories set-up
DB_DIR = "../req_files/"
META_FILE = "../req_files/metadata.txt"


def main():
    _init_metadata(META_FILE)
    if len(sys.argv) != 2:
        print("ERROR : invalid args")
        print("USAGE : python {} '<sql query>'".format(sys.argv[0]))
        exit(-1)
    q = sys.argv[1]

    qdict = _parse_query(q)
    out_header, out_table = _get_op_tbl(qdict)

    _print_table(out_header, out_table)


if __name__ == "__main__":
    main()
