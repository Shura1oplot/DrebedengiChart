#!/usr/bin/env python3
# [SublimeLinter @python:3]

import sys
import os
import codecs
import io
import re
import datetime
import csv
import sqlite3
import zipfile
import argparse
import requests


def load_credentials(file):
    with open(file, "r", encoding="utf-8") as fp:
        content = fp.read()

    login, password = content.split("\n", 1)
    return login, password.rstrip("\n")


def download_backup(login, password):
    s = requests.Session()
    r = s.post("https://www.drebedengi.ru/?module=v2_start&action=login",
               data={"o": "1",
                     "email": login,
                     "password": password,
                     "ssl": "on"})
    r.raise_for_status()
    r = s.post("https://www.drebedengi.ru/?module=v2_homeBuhPrivateExport",
               data={"action": "do_archive",
                     "password": password,
                     "exportType": "1",
                     "is_sent_backup": "true"})
    r.raise_for_status()
    r = s.get("https://www.drebedengi.ru/?module=v2_homeBuhPrivateExport&action=dwnld")
    r.raise_for_status()

    z = zipfile.ZipFile(io.BytesIO(r.content))
    filenames = z.namelist()

    if len(filenames) != 1:
        raise ValueError(filenames)

    txtfname = filenames[0]

    d = datetime.datetime.strptime(txtfname, "%Y-%m-%d_%H_%M_%S.txt")

    if datetime.datetime.today() - d > datetime.timedelta(hours=1):
        raise ValueError(txtfname)

    # === split file ===

    fpout = {}
    section = None

    for line in codecs.getreader("utf-8")(z.open(txtfname)):

        if line.startswith("[") and line.rstrip().endswith("]"):
            section = line.rstrip()[1:-1]
            fpout[section] = io.StringIO()
            continue

        fpout[section].write(line)

    for fp in fpout.values():
        fp.seek(0)

    # === parse data ===

    data = {}

    for section, fp in fpout.items():
        rows = []
        csvfile = csv.reader(fp, delimiter=";")

        for row in csvfile:
            rows.append(row)

        data[section] = rows

    for fp in fpout.values():
        fp.close()

    return data


def init_database(database, data):
    conn = sqlite3.connect(database)

    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE currency (
            [id] int NOT NULL,
            [name] text,
            [course] numeric(18,4),
            [code] text,
            [is_autoupdate] int,
            [is_hidden] int,
            [is_default] int
        );
    """)
    cur.execute("""
        CREATE TABLE objects (
            [id] int NOT NULL,
            [parent_id] int,
            [type] int,
            [name] text,
            [user_id] int,
            [is_credit_card] int,
            [is_hidden] int,
            [is_for_duty] int,
            [sort] int,
            [icon_id] int,
            [is_autohide] int
        );
    """)
    cur.execute("""
        CREATE TABLE records (
            [id] int NOT NULL,
            [sum] int,
            [currency_id] int,
            [object_id] int,
            [account_id] int,
            [date] text,
            [comment] text,
            [user_id] int,
            [group_id] int
        );
    """)
    cur.execute("""
        CREATE TABLE tags (
            [record_id] int NOT NULL,
            [tag] text NOT NULL
        );
    """)
    conn.commit()

    for id_, name, course, code, is_autoupdate, is_hidden, is_default \
            in data["currency"]:
        cur.execute("INSERT INTO currency VALUES (?,?,?,?,?,?,?)",
                    (id_,
                     name,
                     course,
                     code,
                     {"t": 1, "f": 0}[is_autoupdate],
                     {"t": 1, "f": 0}[is_hidden],
                     {"t": 1, "f": 0}[is_default],
                     ))

    for id_, parent_id, type_, name, user_id, is_credit_card, is_hidden, \
            is_for_duty, sort, icon_id, is_autohide in data["objects"]:
        cur.execute("INSERT INTO objects VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (id_,
                     parent_id,
                     type_,
                     name,
                     user_id,
                     {"t": 1, "f": 0}[is_credit_card],
                     {"t": 1, "f": 0}[is_hidden],
                     {"t": 1, "f": 0}[is_for_duty],
                     sort,
                     icon_id,
                     {"t": 1, "f": 0}[is_autohide],
                     ))

    for id_, (sum_, currency_id, object_id, account_id, date, \
            comment, user_id, group_id) in enumerate(data["records"]):
        cur.execute("INSERT INTO records VALUES (?,?,?,?,?,?,?,?,?)",
                    (id_,
                     sum_,
                     currency_id,
                     object_id,
                     account_id,
                     date,
                     comment,
                     user_id,
                     group_id,
                     ))

    for id_, (_, _, _, _, _, comment, _, _) in enumerate(data["records"]):
        for tag in re.findall(r"\[([^[\]]+)\]", comment):
            cur.execute("INSERT INTO tags VALUES (?,?)", (id_, tag))

    conn.commit()
    cur.close()
    conn.close()


def main(argv=sys.argv):
    parser = argparse.ArgumentParser(description="Drebedengi Chart")

    parser.add_argument(
        "-c", "--credentials",
        default="credentials.txt",
        metavar="FILE",
        help=("file with the login and password for drebedengi.ru"
              "(default: credentials.txt)"))

    parser.add_argument(
        "-d", "--database",
        default="drebedengi.sqlite3",
        metavar="FILE",
        help="database file (default: drebedengi.sqlite3)")

    args = parser.parse_args(argv[1:])

    if os.path.exists(args.database):
        os.remove(args.database)

    init_database(
        args.database,
        download_backup(*load_credentials(args.credentials)))


if __name__ == "__main__":
    sys.exit(main())
