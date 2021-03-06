#!/usr/bin/env python3

"""
        Copyright 2016 Stuart Freeman Licensed under the
	Educational Community License, Version 2.0 (the "License"); you may
	not use this file except in compliance with the License. You may
	obtain a copy of the License at

http://www.osedu.org/licenses/ECL-2.0

	Unless required by applicable law or agreed to in writing,
	software distributed under the License is distributed on an "AS IS"
	BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
	or implied. See the License for the specific language governing
	permissions and limitations under the License.
"""

import csv, datetime, errno, getopt, io, os, re, sqlite3, sys, zipfile

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def main(argv):
    # If start and end date aren't specified we'll use the first and last days of the previous month
    startDate = (datetime.date.today().replace(day=1) - datetime.timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
    endDate = (datetime.date.today().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    # Set up command line parsing
    usage = sys.argv[0] + ' -i <inputfile> -s <YYYY-MM-DD> -e <YYYY-MM-DD>'
    try:
        opts, args = getopt.getopt(argv, "hi:s:e:", ['ifile', 'startDate', 'endDate'])
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
        elif opt == '-s':
            if re.fullmatch('\d{4}-\d{2}-\d{2}', arg):
                startDate = arg
            else:
                eprint('Dates should be in the format YYYY-MM-DD')
                sys.exit(2)
        elif opt == '-e':
            if re.fullmatch('\d{4}-\d{2}-\d{2}', arg):
                endDate = arg
            else:
                eprint('Dates should be in the format YYYY-MM-DD')
                sys.exit(2)

    # Make the results dir
    resultspath = os.path.dirname(os.path.realpath(__file__)) + '/results'
    try:
        os.makedirs(resultspath)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    dbfile = resultspath + '/coursera.sqlite3'
    dbExists = os.path.isfile(dbfile)
    try:
        dbOld = getFileModDate(inputfile) > getFileModDate(dbfile) if dbExists else False
    except FileNotFoundError:
        eprint('No such file: ' + inputfile)
        sys.exit(2)

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
            parse('course_grades', cur, myzip)
            parse('users_courses__certificate_payments', cur, myzip)

        cur.execute('CREATE INDEX course_idx1 ON courses(course_id);')
        cur.execute('CREATE INDEX course_idx2 ON course_memberships(course_id);')
        cur.execute('CREATE INDEX course_idx3 ON course_grades(course_id);')
        cur.execute('CREATE INDEX date_idx1 ON course_memberships(course_membership_ts);')
        db.commit()

    with open('{0}/report.{1}.{2}.csv'.format(resultspath, startDate, endDate), 'w') as reportfile:
        writer = csv.writer(reportfile)
        cur.execute("""SELECT courses.course_name,
        count(DISTINCT course_memberships.gatech_user_id) AS members,
        count(CASE course_memberships.course_membership_role WHEN 'LEARNER' THEN 1 ELSE null END) AS active,
        payments.paid AS paid,
        payments.finaid AS finaid,
        completed.completed AS completed
        FROM courses
                JOIN course_memberships ON courses.course_id = course_memberships.course_id
                JOIN (
                    SELECT course_memberships.course_id, count(DISTINCT (CASE users_courses__certificate_payments.was_payment WHEN  't' THEN course_memberships.gatech_user_id END)) AS paid, count(DISTINCT (CASE users_courses__certificate_payments.was_finaid_grant WHEN 't' THEN course_memberships.gatech_user_id END)) AS finaid FROM users_courses__certificate_payments JOIN course_memberships ON users_courses__certificate_payments.course_id = course_memberships.course_id AND users_courses__certificate_payments.gatech_user_id = course_memberships.gatech_user_id WHERE course_membership_ts BETWEEN ? AND ? GROUP BY course_memberships.course_id
                ) AS payments ON courses.course_id = payments.course_id
                JOIN (
                    SELECT course_grades.course_id, count(DISTINCT course_memberships.gatech_user_id) AS completed FROM course_grades JOIN course_memberships ON course_grades.course_id = course_memberships.course_id AND course_grades.gatech_user_id = course_memberships.gatech_user_id WHERE (course_grades.course_passing_state_id = '1' OR course_grades.course_passing_state_id = '2') AND course_memberships.course_membership_ts BETWEEN ? AND ? GROUP BY course_memberships.course_id
                ) AS completed ON courses.course_id = completed.course_id
                WHERE course_memberships.course_membership_ts BETWEEN ? AND ? GROUP BY courses.course_id;""", (startDate, endDate, startDate, endDate, startDate, endDate))
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
