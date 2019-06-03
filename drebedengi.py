#!/usr/bin/env python3
# [SublimeLinter @python:3]

import sys
import os
import codecs
import io
import datetime
import json
import csv
import sqlite3
import zipfile
import subprocess
import argparse
import requests


MONTH_NAMES = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль",
               "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]


html_template = """
<html>
    <meta charset='UTF-8' name='viewport' content='width=device-width, initial-scale=1.0, maximum-scale=1.0, minimum-scale=1.0, user-scalable=no, target-densityDpi=device-dpi' />
    <head>
        <script type='text/javascript' src='https://www.gstatic.com/charts/loader.js'></script>
        <script type='text/javascript'>
            google.charts.load('current', {packages:['corechart']});
            google.charts.setOnLoadCallback(drawChart);

            function drawChart() {
              var data = google.visualization.arrayToDataTable(%s);
              var options = {
                title: %s,
                legend: {position: 'top'},
                // height: 400,
                colors: ['#1b9e77', '#d95f02', '#39648c', '#6d398c', '#8c3939'],
                vAxis: {
                  viewWindow: {
                    min: 0
                  }
                }
              };
              var chart = new google.visualization.ColumnChart(
                document.getElementById('chart_div'));
              chart.draw(data, options);
            };

            window.onresize = drawChart;
        </script>
    </head>
    <body>
        <div id='chart_div' style='width: 100%%; height: 100%%;'></div>
    </body>
</html>
""".strip()


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


def init_database(conn, data):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE currency (
            [id] int not null,
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
            [id] int not null,
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

    for sum_, currency_id, object_id, account_id, date, comment, user_id, \
            group_id in data["records"]:
        cur.execute("INSERT INTO records VALUES (?,?,?,?,?,?,?,?)",
                    (sum_,
                     currency_id,
                     object_id,
                     account_id,
                     date,
                     comment,
                     user_id,
                     group_id,
                     ))

    conn.commit()
    cur.close()


def load_query(file):
    with open(file, "r", encoding="utf-8") as fp:
        content = fp.read()

    try:
        header, sql = content.split("\n\n", 1)
    except ValueError:
        raise ValueError("invalid query file")

    fields = []

    for i, line in enumerate(header.split("\n")):
        line = line.strip()

        if i == 0:
            if line.lower() != "-- drebedengi chart":
                raise ValueError("invalid query file")

            continue

        if not line:
            continue

        field = line[2:].strip()

        if not field:
            continue

        fields.append(field)

    if not fields or not sql:
        raise ValueError("invalid query file")

    return fields, sql


def query_data(conn, sql, fields, n, mode):
    cur = conn.cursor()

    result = []

    for date0, date1 in date_iter(mode, n):
        date0_str = "{:%Y-%m-%d %H:%M:%S}".format(date0)
        date1_str = "{:%Y-%m-%d %H:%M:%S}".format(date1)

        item = [date1.year, date1.month]

        for field in fields:
            cur.execute(sql, (field, date0_str, date1_str))
            row = cur.fetchone()
            item.append(round((row[0] or 0) / 100, 2))

        result.append(item)

    return result


def date_iter(mode, n):
    if mode == 1:
        return date_iter_by_month(n)
    elif mode == 2:
        return date_iter_by_year(n)
    elif mode == 3:
        return date_iter_by_year_aggr(n)


def date_iter_by_month(n):
    dates0 = []

    today = datetime.datetime.today()
    m0 = today.month
    y0 = today.year

    dates0.append(datetime.datetime(
        y0 + m0 // 12,
        m0 % 12 + 1,
        1, 0, 0, 0
    ))

    for i in range(n):
        m1 = (m0 - i - 1) % 12 + 1
        y1 = y0 - (i + 12 - m0) // 12
        dates0.append(datetime.datetime(y1, m1, 1, 0, 0, 0))

    date1 = iter(dates0)
    next(date1)

    return zip(dates0, date1)


def date_iter_by_year(n):
    today = datetime.datetime.today()
    y0 = today.year

    for i in range(n):
        yield (
            datetime.datetime(y0 - i, 1, 1, 0, 0, 0),
            datetime.datetime(y0, 1, 1, 0, 0, 0),
        )


def date_iter_by_year_aggr(n):
    today = datetime.datetime.today()
    y0 = today.year

    tomorrow = today + datetime.timedelta(days=1)
    d1 = tomorrow.day
    m1 = tomorrow.month
    y1 = tomorrow.year

    for i in range(n):
        yield (
            datetime.datetime(y0 - i, 1, 1, 0, 0, 0),
            datetime.datetime(y1 - i, m1, d1, 0, 0, 0),
        )


def get_chart_html(data, fields, title):
    header = ["Месяц", ]

    for field in fields:
        header.append(field)
        header.append({"role": "annotation"})

    json_data = [header, ]

    for row in data:
        y, m = row[:2]

        item = []
        item.append("{}'{}".format(MONTH_NAMES[m-1], str(y)[-2:]))

        for x in row[2:]:
            item.append(int(round(x)))
            item.append("{}k".format(int(round(x / 1000))))

        json_data.append(item)

    return html_template % (json.dumps(title), json.dumps(json_data),)


def open_in_brewser(html_file):
    if sys.platform == "win32":
        os.startfile(html_file)

    elif sys.platform == "linux":
        subprocess.call(("xdg-open", html_file))

    elif sys.platform == "ios":  # pythonista
        import ui

        webview = ui.WebView(name="Drebedengi Chart")

        with open(html_file, "r", encoding="utf-8") as fp:
            webview.load_html(fp.read())

        webview.present()

    else:
        raise ValueError(sys.platform)


def main(argv=sys.argv):
    parser = argparse.ArgumentParser(description="Drebedengi Chart")

    parser.add_argument(
        "-m", "--mode",
        type=int,
        choices=(1, 2, 3),
        default=1,
        metavar="N",
        help=("mode: 1 - by month, 2 - by year, 3 - by year up to today "
              "(default: 1)"))

    parser.add_argument(
        "-c", "--credentials",
        default="credentials.txt",
        metavar="FILE",
        help=("file with the login and password for drebedengi.ru"
              "(default: credentials.txt)"))

    parser.add_argument(
        "-d", "--database",
        metavar="FILE",
        help="database file (in memory if not specified)")
    parser.add_argument(
        "-u", "--update",
        action="store_true",
        help="download new backup from the drebedengi.ru")

    parser.add_argument(
        "-n", "--number",
        default=2,
        type=int,
        metavar="N",
        help="a number of months to show (default: 2)")

    parser.add_argument(
        "-s", "--save-html",
        metavar="FILE",
        help="save html output into the file")
    parser.add_argument(
        "-j", "--save-json",
        metavar="FILE",
        help="save json output into the file")

    parser.add_argument(
        "-x", "--open",
        action="store_true",
        help=("show the chart in OS default browser "
              "(must be used with `--save-json`)"))

    parser.add_argument(
        "query",
        metavar="FILE",
        help="chart query file (special header + SQL)")

    args = parser.parse_args(argv[1:])

    if args.number <= 0:
        raise ValueError("`--number` must be greater than or equal to 1")

    fields, sql = load_query(args.query)

    if args.update:
        need_update = True

    elif not args.database:
        need_update = True

    elif not os.path.exists(args.database):
        need_update = True

    else:
        need_update = False

    if need_update and args.database and os.path.exists(args.database):
        os.remove(args.database)

    conn = sqlite3.connect(args.database or ":memory:")

    if need_update:
        login, password = load_credentials(args.credentials)
        backup_data = download_backup(login, password)
        init_database(conn, backup_data)

    result = query_data(conn, sql, fields, args.number, args.mode)
    conn.close()

    if args.save_json:
        with open(args.save_json, "w", encoding="utf-8") as fp:
            json.dump(result, fp)

    if args.save_html:
        with open(args.save_html, "w", encoding="utf-8") as fp:
            fp.write(get_chart_html(
                result,
                fields,
                "mode: {}".format(args.mode)
            ))

        if args.open:
            open_in_brewser(args.save_html)


if __name__ == "__main__":
    sys.exit(main())
