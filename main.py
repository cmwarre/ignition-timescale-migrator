#!/usr/bin/python
import datetime
import os
import sys
import mysql.connector
from optparse import OptionParser

def main(argv):
    # Setup command line argument parsing
    usage = "usage: %prog [options] directory"
    parser = OptionParser(usage=usage)
    parser.add_option("-u", "--user", dest="username", help="MySQL Username")
    parser.add_option("-p", "--password", dest="password", help="MySQL Password")
    parser.add_option("--host", dest="host", help="MySQL Host Address")
    parser.add_option("-d", "--database", dest="database", help="MySQL Database Name")

    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("Error: Incorrect number of arguments")

    # Read in arguments
    if options.host is None:
        host = "127.0.0.1"
    else:
        host = options.host

    if options.username is None:
        username = "root"
    else:
        username = options.username

    if options.password is None:
        password = ""
    else:
        password = options.password

    if options.database is None:
        database = "Historian"
    else:
        database = options.database

    basedir = args[0]
    backupdir = basedir #+ 'mysql.' + database + "." + str(datetime.date.today())

    # Connect to MySQL
    db = mysql.connector.connect(
        host=host,
        user=username,
        passwd=password,
        database=database
    )

    cursor = db.cursor()

    # Check if the backups table exists.
    query = "SHOW TABLES WHERE tables_in_%s = 'sqlth_backups';" % database

    cursor.execute(query)
    results = cursor.fetchall()

    # If there isn't a backups table, create it.
    if len(results) < 1:
        print("Creating backups table.")

        query = """
            CREATE TABLE sqlth_backups (
                backupid int(11) NOT NULL AUTO_INCREMENT,
                target varchar(255) DEFAULT NULL,
                actiontime datetime NOT NULL,
                expires datetime NOT NULL,
                PRIMARY KEY (backupid));"""

        cursor.execute(query)
        db.commit()
    # end if

    # Select the current history partition to avoid backing that up.
    query = """
			SELECT
				pname
			FROM sqlth_partitions
			WHERE 
				end_time = (SELECT MAX(end_time) FROM sqlth_partitions);
			"""

    cursor.execute(query)
    latestPartition = cursor.fetchall()[0][0]

    # Select misc history tables and history partitions that haven't been backed up yet
    query = """
			SHOW TABLES
			WHERE
				tables_in_{db} LIKE 'sqlth%'
			OR (
				tables_in_{db} LIKE 'sqlt_data_%'
			AND
				tables_in_{db} not in (select distinct target from sqlth_backups)
			);
			""".format(db=database)

    cursor.execute(query)
    tableList = cursor.fetchall()

    # create a new directory for this set of backups
    os.system("mkdir %s" % backupdir)

    # For each table, back it up to it's own SQL file, then insert a record into the backups
    # table to record the backup.
    queries = []
    for row in tableList:
        if row[0] != latestPartition:
            print("Backing up " + row[0])

            os.system("mysqldump -h {host} -u {user} -p{passwd} {db} {table} --no-data > {backupdir}/{table}.sql".format(
                host=host,
                user=username,
                passwd=password,
                db=database,
                table=row[0],
                backupdir=backupdir
            ))

            os.system("mysqldump -h {host} -u {user} -p{passwd} {db} {table} -t --fields-terminated-by=, -T {backupdir}".format(
                host=host,
                user=username,
                passwd=password,
                db=database,
                table=row[0],
                backupdir=backupdir
            ))


            query = "INSERT INTO sqlth_backups(target, actiontime) VALUES('{0}', CURRENT_TIMESTAMP);".format(row[0])
            cursor.execute(query)
    # end for

    # Commit and close the database connection
    db.commit()
    cursor.close()
    db.close()

    # Archive the backup to a .tar.gz and remove the temporary backup folder.
    print("Zipping Archives")

    #os.system("tar -cvzf %s.tar.gz %s" % (backupdir, backupdir))
    #os.system("rm -r %s" % backupdir)


# end def


if __name__ == "__main__":
    main(sys.argv[1:])
