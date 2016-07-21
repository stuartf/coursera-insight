#!/usr/bin/env python3

import csv, datetime, getopt, io, os, sqlite3, sys, zipfile

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def main(argv):
    # Set up command line parsing
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

    dbfile = os.path.dirname(os.path.realpath(__file__)) + '/coursera.sqlite3'
    dbExists = os.path.isfile(dbfile)
    dbOld = getFileModDate(inputfile) > getFileModDate(dbfile)

    # Our cached DB is old, delete it so we can rebuild
    if dbOld:
        os.remove(dbfile)

    # Connect to the cached DB or create an empty one
    db = sqlite3.connect(dbfile)
    cur = db.cursor()

    # If we just created an empty DB populate it from the zip file
    if not dbExists or dbOld:
        with zipfile.ZipFile(inputfile) as myzip:
            parse('courses', cur, myzip)
            parse('course_memberships', cur, myzip)
            parse('users', cur, myzip)
            parse('users_courses__certificate_payments', cur, myzip)

        db.commit()

    # TODO add start and end date to filename
    with open('report.csv', 'w') as reportfile:
        writer = csv.writer(reportfile)
        cur.execute('SELECT courses.course_name, count(course_memberships.gatech_user_id) AS members FROM courses JOIN course_memberships ON courses.course_id = course_memberships.course_id GROUP BY courses.course_id;')
        writer.writerow([d[0] for d in cur.description])
        writer.writerows(cur.fetchall())

def parse(name, cur, zipfile):
    """Extract a csv from the zip and read it into a table"""
    csvfile = io.TextIOWrapper(zipfile.open(name + '.csv'), encoding='utf-8')
    dr = csv.DictReader(csvfile)
    cur.execute('CREATE TABLE {0} ({1});'.format(name, ', '.join(dr.fieldnames)))
    for row in dr:
        columns = ', '.join([':' + key for key in filter(None, row.keys())])
        cur.execute('INSERT INTO {0} ({1}) VALUES ({2});'.format(name, ', '.join(filter(None,row.keys())), columns), row)

def getFileModDate(filename):
    """Get the datetime when a file was modified"""
    t = os.path.getmtime(filename)
    return datetime.datetime.fromtimestamp(t)

if __name__ == '__main__':
    main(sys.argv[1:])
