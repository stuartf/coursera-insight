#!/usr/bin/env python3

import csv, datetime, getopt, io, sqlite3, sys, zipfile

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def main(argv):
    usage = sys.argv[0] + ' -i <inputfile>'
    try:
        opts, args = getopt.getopt(argv, "hi:", ['ifile'])
    except getopt.GetoptError:
        print(usage)
        sys.exit(2)
    if len(opts) == 0:
        print(usage)
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print(usage)
            sys.exit()
        elif opt == '-i':
            inputfile = arg

    db = sqlite3.connect(':memory:')
    cur = db.cursor()

    with zipfile.ZipFile(inputfile) as myzip:
        parse('courses', cur, myzip)
        parse('course_memberships', cur, myzip)
        parse('users', cur, myzip)
        parse('users_courses__certificate_payments', cur, myzip)

    db.commit()

def parse(name, cur, zipfile):
    csvfile = io.TextIOWrapper(zipfile.open(name + '.csv'), encoding='utf-8')
    dr = csv.DictReader(csvfile)
    cur.execute('CREATE TABLE {0} ({1});'.format(name, ', '.join(dr.fieldnames)))
    for row in dr:
        columns = ', '.join([':' + key for key in filter(None, row.keys())])
        cur.execute('INSERT INTO {0} ({1}) VALUES ({2});'.format(name, ', '.join(filter(None,row.keys())), columns), row)

if __name__ == '__main__':
    main(sys.argv[1:])
