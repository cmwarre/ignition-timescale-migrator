#!/usr/bin/python
import csv
import os
import threading

import mysql.connector
import numpy
import psycopg
from optparse import OptionParser


def main():
    """
    Exports tables from a MySQL based Ignition historian and imports them into a TimescaleDB instance
    """

    # Setup command line argument parsing
    usage = "usage: %prog [options] action"
    parser = OptionParser(description=main.__doc__, usage=usage)
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


# <editor-fold desc="Export">

def export_data(host, database, username, password, backupdir, compress=False, threads=1):
    """
    Exports data from a mysql database to a specified directory
    :param threads: number of threads to utilize
    :param compress: compress the data
    :param host: MySQL host
    :param database: MySQL Database
    :param username: MySQL Username
    :param password: MySQL Password
    :param backupdir: Backup Directory
    :return: None
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
                                    args=(table_groups[i], host, database, username, password, backupdir,
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


def verify_backup_table(db, con, database):
    """
    Verify that the sqlth_backups table exists in the MySQL historian database and creates if it not
    :param db: Database Connection
    :param con: Database connection cursor
    :param database: Database Name
    :return: None
    """

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


def get_latest_partition(con):
    """
    Returns the name of the latest partition in an Ignition historian database
    :param con: MySQL Connection Cursor
    :return String: name of the latest partition in a database
    """
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


def get_tables(con, database):
    """
    Return a list of table names from a MySQL database
    :param con: MySQL connection
    :param database: MySQL database name
    :return List: list of strings with table names
    """

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
    """
    creates a connection to MYSQL, spawns threads and has each begin exporting Mysql table data to .csv files
    :param tables: list of tables to dump
    :param host: MySQL host
    :param database: MySQL database name
    :param username: MySQL user
    :param password: MySQL password
    :param backupdir: Directory to dump mysql data .csv files
    :param latest_partition: Last partition in the table (to be avoided because it could be being written to)
    :return: None
    """
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
            dump_table_to_csv(con, table, backupdir)
    # end for

    # Commit and close the database connection
    db.commit()
    con.close()
    db.close()


def dump_table_to_csv(con, table, backupdir):
    """
    Dump a mysql table to a csv file and a structure .sql file
    :param con: MySQL connection to use
    :param table: Table name to dump data for
    :param backupdir: Directory to store backup files
    :return: None
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


# </editor-fold>

# <editor-fold desc="Import">


def import_data(host, database, username, password, backupdir, compress, threads,
                chunk_size_days, compress_after_days, retain_days):
    """
    Import data into TimescaleDB database
    :param host:
    :param database:
    :param username:
    :param password:
    :param backupdir:
    :param compress:
    :param threads:
    :param chunk_size_days:
    :param compress_after_days:
    :param retain_days:
    :return:
    """

    con_string = "dbname={database} user={username} password={password} host={host} port=5432 sslmode=require".format(
        database=database,
        username=username,
        password=password,
        host=host
    )

    # todo handle file compression

    # todo gracefully fail at creating tables
    print("Creating tables in database: {}".format(database))
    create_tables(con_string, chunk_size_days, compress_after_days, retain_days)
    tables = get_list_of_exported_tables(backupdir)
    table_groups = numpy.array_split(tables, threads)
    thread_pool = [threading.Thread(target=import_tables, args=(con_string, table_groups[i], backupdir))
                   for i in range(threads)]

    for thread in thread_pool:
        thread.start()

    # todo fix partition table
    print("Recreating partition tables")


def create_tables(connection_string, chunk_size_days, compress_after_days, retain_days):
    """
    Generate ignition historian tables in PostgreSQL

    :param connection_string: postgresql connection string
    :param chunk_size_days: TimescaleDB chunk size in days
    :param compress_after_days: TimescaleDB compress after days
    :param retain_days: TimescaleDB prune after days
    :return: None
    """
    with psycopg.connect(connection_string) as conn:
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
        "SELECT create_hypertable('sqlth_1_data', 't_stamp', chunk_time_interval=>{}, migrate_data=>true);".format(
            int(chunk_size_days * 86400000)),
        "ALTER TABLE sqlth_1_data set(timescaledb.compress, timescaledb.compress_segmentby = 'tagid');",
        """CREATE OR REPLACE FUNCTION unix_now() returns BIGINT LANGUAGE SQL STABLE as $$ 
        SELECT 1000*extract(epoch from now())::BIGINT $$;""",
        "SELECT set_integer_now_func('sqlth_1_data', 'unix_now');",
        "SELECT add_compression_policy('sqlth_1_data', {});".format(int(compress_after_days * 86400000)),
    ]

    if retain_days > 0:
        queries += "SELECT add_retention_policy('sqlth_1_data', {});".format(int(retain_days * 86400000))

    cur.execute("\n".join(queries))
    conn.commit()


def get_list_of_exported_tables(backupdir):
    """
    List exported MySQL tables from the backup directory
    :param backupdir: path to the MySQL backup directory
    :return: List of strings for exported tables in a backup directory
    """
    onlyfiles = [f for f in os.listdir(backupdir) if os.path.isfile(os.path.join(backupdir, f))]
    return onlyfiles


def import_tables(connection_string, tables, backupdir):
    """
    Import table data for each export in the mysql backup directory into PostgreSQL
    :param connection_string: PostgreSQL connection string
    :param tables: list of tables to import
    :param backupdir: directory of MySQL backup
    :return: None
    """
    import re
    with psycopg.connect(connection_string) as conn:

        for table in tables:
            if match := re.search(r"sqlt(h)?_(\d+|data)_(\d+)?(_)?(.*)", table):
                print("Importing data file {}".format(match.group(0)))
                import_data_file(conn, backupdir, table)
            else:
                print("Importing meta file {}".format(table))
                import_meta_file(conn, backupdir, table)


def import_meta_file(conn, backupdir, file):
    """
    Import meta information table files
    :param conn: postgresql connection cursor
    :param backupdir: MySQL backup directory
    :param file: filepath to import
    :return: None
    """
    try:
        cur = conn.cursor()

        with open(os.path.join(backupdir, file)) as f:
            reader = csv.reader(f)
            columns = next(reader)
            table = file.replace(".csv", "")
            cur.copy_from(f, table, columns=tuple(columns), sep=",", null="")
    except Exception as e:
        print(e)
        pass

        conn.commit()


def import_data_file(conn, backupdir, file):
    """
    Import csv data to postgresql from a provided filepath
    :param conn: TimescaleDB Database Connection
    :param backupdir: MySQL Backup Directory
    :param file: File name to import
    :return: None
    """
    cur = conn.cursor()

    with open(os.path.join(backupdir, file)) as f:
        table = "sqlth_1_data"
        cur.copy_expert("COPY {table} from stdin with (FORMAT CSV)".format(table=table), f)

    conn.commit()


# </editor-fold>


if __name__ == "__main__":
    main()
