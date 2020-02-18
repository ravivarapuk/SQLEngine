import sys
from src.deps import sql_parser as sp

# directories set-up
DB_DIR = "../req_files/"
META_FILE = "../req_files/metadata.txt"


def main():
    sp._init_metadata(META_FILE)
    if len(sys.argv) != 2:
        print("ERROR : invalid args")
        print("USAGE : python {} '<sql query>'".format(sys.argv[0]))
        exit(-1)
    q = sys.argv[1]

    qdict = sp._parse_query(q)
    out_header, out_table = sp._get_op_tbl(qdict)

    sp._print_table(out_header, out_table)


if __name__ == "__main__":
    main()
