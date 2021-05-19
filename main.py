#!/usr/bin/python
import csv
import os
import sys
import threading

import mysql.connector
import numpy
import psycopg2
from optparse import OptionParser


def main(argv):
    # Setup command line argument parsing
    usage = "usage: %prog [options] action"
    parser = OptionParser(usage=usage)
    parser.add_option("-u", "--user", dest="username", help="MySQL Username")
    parser.add_option("-p", "--password", dest="password", help="MySQL Password")
    parser.add_option("-H", "--host", dest="host", help="MySQL Host Address")
    parser.add_option("-d", "--database", dest="database", help="MySQL Database Name")
    parser.add_option("-t", "--threads", dest="threads", help="Number of threads")
    parser.add_option("-b", "--backupdir", dest="backupdir", help="Backup Directory")
    parser.add_option("-z", "--compress", dest="compress", help="Compress Backup")
    parser.add_option("-c", "--chunk", dest="chunk_size_days", help="Hyper Table Chunk Size in Days")
    parser.add_option("-C", "--compression", dest="compress_after_days", help="# of Days After Insertion to Compress")
    parser.add_option("-r", "--retain", dest="retain_days", help="# of Days to Retain Data")

    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("Error: Incorrect number of arguments")

    # Read in arguments
    host = "127.0.0.1" if options.host is None else options.host
    username = "root" if options.username is None else options.username
    password = options.password
    database = "Historian" if options.database is None else options.database
    threads = 1 if options.threads is None else int(options.threads)
    backupdir = "" if options.backupdir is None else options.backupdir
    compress = False if options.compress is None else True
    chunk_size_days = 1 if options.chunk_size_days is None else int(options.chunk_size_days)
    compress_after_days = 1 if options.compress_after_days is None else int(options.compress_after_days)
    retain_days = 0 if options.retain_days is None else int(options.retain_days)

    action = args[0]

    if action == "export":
        export_data(host, database, username, password, backupdir, compress, threads)
    if action == "import":
        import_data(host, database, username, password, backupdir, compress, threads,
                    chunk_size_days, compress_after_days, retain_days)
    # end if


# end def

# <editor-fold desc="Export">


def export_data(host, database, username, password, backupdir, compress=False, threads=1):
    """
    :param threads:
    :param compress:
    :param host:
    :param database:
    :param username:
    :param password:
    :param backupdir:
    :return:
    """

    # Connect to MySQL
    db = mysql.connector.connect(
        host=host,
        user=username,
        passwd=password,
        database=database
    )

    con = db.cursor()

    verify_backup_table(db, con, database)
    latest_partition = get_latest_partition(con)
    tables = get_tables(con, database)

    # Commit and close the database connection
    db.commit()
    con.close()
    db.close()

    # For each table, back it up to it's own SQL file, then insert a record into the backups
    # table to record the backup.
    table_groups = numpy.array_split(tables, threads)
    thread_pool = [threading.Thread(target=dump_tables,
                                    args=(
                                        table_groups[i], host, database, username, password, backupdir,
                                        latest_partition))
                   for i in range(threads)]

    for thread in thread_pool:
        thread.start()
    # end for

    if compress:
        # Archive the backup to a .tar.gz and remove the temporary backup folder.
        print("Zipping Archives")
        os.system("tar -cvzf %s.tar.gz %s" % (backupdir, backupdir))
        os.system("rm -r %s" % backupdir)


# end def


def verify_backup_table(db, con, database):
    # Check if the backups table exists.
    query = "SHOW TABLES WHERE tables_in_{0} = 'sqlth_backups';".format(database)

    con.execute(query)
    results = con.fetchall()

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

        con.execute(query)
        db.commit()
    # end if


# end def


def get_latest_partition(con):
    # Select the current history partition to avoid backing that up.
    query = """
    SELECT
        pname
    FROM sqlth_partitions
    WHERE 
        end_time = (SELECT MAX(end_time) FROM sqlth_partitions);
    """

    con.execute(query)
    return con.fetchall()[0][0]


# end def


def get_tables(con, database):
    # Select misc history tables and history partitions that haven't been backed up yet
    query = """
            SHOW TABLES WHERE 
                tables_in_{db} LIKE 'sqlth%' 
                OR ( tables_in_{db} LIKE 'sqlt_data_%' 
                AND  tables_in_{db} not in (select distinct target from sqlth_backups));
            """.format(db=database)

    con.execute(query)
    tables_raw = con.fetchall()
    tables = [r[0] for r in tables_raw]  # convert list of tuples to a single list
    print("Found Tables: {}".format(tables))
    return tables


# end def


def dump_tables(tables, host, database, username, password, backupdir, latest_partition):
    print("Dumping tables {0}".format(str(tables)))
    # Connect to MySQL
    db = mysql.connector.connect(
        host=host,
        user=username,
        passwd=password,
        database=database
    )

    con = db.cursor()

    for table in tables:
        if table != latest_partition:
            dump_table_to_csv(con, table, host, database, username, password, backupdir)
    # end for

    # Commit and close the database connection
    db.commit()
    con.close()
    db.close()


# end def


def dump_table_to_csv(con, table, host, database, username, password, backupdir):
    """
    Dump a mysql table to a csv file and a structure .sql file
    :param con:
    :param table:
    :param host:
    :param database:
    :param username:
    :param password:
    :param backupdir:
    :return:
    """
    print("Exporting " + table)

    query = "select * from {table}".format(table=table)
    con.execute(query)
    data = con.fetchall()

    if data:
        result = list()
        column_names = list()

        for i in con.description:
            column_names.append(i[0])

        result.append(column_names)
        for row in data:
            result.append(row)

        csv_path = os.path.join(backupdir, table + ".csv")

        with open(csv_path, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in result:
                csvwriter.writerow(row)

    query = "INSERT INTO sqlth_backups(target, actiontime) VALUES('{0}', CURRENT_TIMESTAMP);".format(table)
    con.execute(query)


# end def

# </editor-fold>

# <editor-fold desc="Import">


def import_data(host, database, username, password, backupdir, compress, threads,
                chunk_size_days, compress_after_days, retain_days):

    connection_string = "dbname={database} user={username} password={password} host={host} port=5432 sslmode=require".format(
        database=database,
        username=username,
        password=password,
        host=host
    )

    # todo gracefully fail at creating tables
    print("Creating tables in database: {}".format(database))
    create_tables(connection_string, chunk_size_days, compress_after_days, retain_days)
    tables = get_list_of_exported_tables(backupdir)
    table_groups = numpy.array_split(tables, threads)
    thread_pool = [threading.Thread(target=import_tables, args=(connection_string, table_groups[i], backupdir))
                   for i in range(threads)]

    for thread in thread_pool:
        thread.start()

    # todo fix partition table
    print("Recreating partition tables")


# end def


def create_tables(connection_string, chunk_size_days, compress_after_days, retain_days):

    with psycopg2.connect(connection_string) as conn:
        cur = conn.cursor()
        try:
            with open("postgres_tables.sql", 'r') as sqlfile:
                sql = sqlfile.read()
                cur.execute(sql)
                conn.commit()
        except Exception as e:
            print(e)
            pass

    print("Setting up hypertable")
    queries = [
        "SELECT create_hypertable('sqlth_1_data', 't_stamp', chunk_time_interval=>{}, migrate_data=>true);".format(int(chunk_size_days * 86400000)),
        "ALTER TABLE sqlth_1_data set(timescaledb.compress, timescaledb.compress_segmentby = 'tagid');",
        "CREATE OR REPLACE FUNCTION unix_now() returns BIGINT LANGUAGE SQL STABLE as $$ SELECT 1000*extract(epoch from now())::BIGINT $$;",
        "SELECT set_integer_now_func('sqlth_1_data', 'unix_now');",
        "SELECT add_compression_policy('sqlth_1_data', {});".format(int(compress_after_days * 86400000)),
    ]

    if retain_days > 0:
        queries += "SELECT add_retention_policy('sqlth_1_data', {});".format(int(retain_days * 86400000))

    cur.execute("\n".join(queries))
    conn.commit()

# end def


def get_list_of_exported_tables(backupdir):
    onlyfiles = [f for f in os.listdir(backupdir) if os.path.isfile(os.path.join(backupdir, f))]
    print(onlyfiles)
    return onlyfiles


# end def


def import_tables(connection_string, tables, backupdir):
    import re
    with psycopg2.connect(connection_string) as conn:

        for table in tables:
            if match := re.search(r"sqlt(h)?_(\d+|data)_(\d+)?(_)?(.*)", table):
                print("Importing data file {}".format(match.group(0)))
                import_data_file(conn, backupdir, table)
            else:
                print("Importing meta file {}".format(table))
                import_meta_file(conn, backupdir, table)
        # end for


# end def


def import_meta_file(conn, backupdir, file):
    try:
        cur = conn.cursor()

        with open(os.path.join(backupdir, file)) as f:
            reader = csv.reader(f)
            columns = next(reader)
            table = file.replace(".csv", "")
            cur.copy_from(f, table, columns=tuple(columns), sep=",", null="")
    except:
        pass

        conn.commit()


# end def


def import_data_file(conn, backupdir, file):
    cur = conn.cursor()

    with open(os.path.join(backupdir, file)) as f:
        reader = csv.reader(f)
        table = "sqlth_1_data"
        cur.copy_expert("COPY {table} from stdin with (FORMAT CSV)".format(table=table), f)

    conn.commit()


# end def

# </editor-fold>


if __name__ == "__main__":
    main(sys.argv[1:])
