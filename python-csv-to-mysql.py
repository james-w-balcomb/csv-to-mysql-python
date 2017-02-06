# import ConfigParser #
import configparser
import csv          #
# import MySQLdb      #
# import mysqlclient
import pymysql      # connect, cursor, execute, commit, fetchone, close
import os           # os.listdir, os.path.isdir, os.path.join
import sys          # argv, exit
# import glob         # glob
import fnmatch      # fnmatch
import math         #
import datetime

DEBUG = False
DEBUG_VERBOSE = False

__author__ = "James W. Balcomb"

# SQL_USE_DATABASE = 'USE %s;' % directory_for_schema_name
# SQL_CHECK_FOR_DATABASE = 'SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = %s;'
# SQL_DROP_DATABASE = 'DROP DATABASE %s;' % directory_for_schema_name
# SQL_CREATE_DATABASE = 'CREATE DATABASE %s;' % directory_for_schema_name
# SQL_USE_DATABASE = 'USE %s;'
# SQL_CHECK_FOR_DATABASE = 'SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = %s;'
# SQL_DROP_DATABASE = 'DROP DATABASE %s;'
# SQL_CREATE_DATABASE = 'CREATE DATABASE %s;'


def list_of_directories(directory):

    return [
        d for d in os.listdir(directory)
        if os.path.isdir(os.path.join(directory, d))
        ]


# def list_of_directories_full_path(directory):
#
#     return [
#         d for d in (os.path.join(directory, d1)
#                     for d1 in os.listdir(directory))
#         if os.path.isdir(d)
#         ]


def list_of_files(directory):

    return [
        d for d in os.listdir(directory)
        if os.path.isfile(os.path.join(directory, d))
        ]


# def is_integer(x):
#
#     try:
#         int(x)
#         return True
#
#     except ValueError:
#         return False


def is_float(x):

    try:
        float(x)
        return True

        # except TypeError:
        # return False

    except ValueError:
        return False


def is_integer_number(n):

    if float(n) % 1 == 0:
        return True

    else:
        return False


def is_unsigned_number(n):

    if float(n) >= 0:
        return True

    else:
        return False


def get_digit_count(n):

    if float(n) > 0:
        digits = int(math.log10(n)) + 1

    elif float(n) == 0:
        digits = 1

    elif float(n) < 0:
        digits = int(math.log10(-n)) + 2  # +1 if you don't count the '-'

    else:
        digits = None

    return digits


def main(argv):

    print('{0:%Y-%m-%d %H:%M:%S} : Beginning...'
          .format(datetime.datetime.now()))

    if DEBUG:
        print('argv: ' + str(argv))
        print('argv[0]: ' + str(argv[0]))

    # configuration_file_path = 'C:/000000/Development/PyMyCsv2Db/'
    # configuration_file_name = 'PyMyCsv2Db.cnf'
    # configuration_file = 'C:/000000/Development/PyMyCsv2Db/PyMyCsv2Db.cnf'
    configuration_file = 'python-csv-to-mysql.config'

    # print('configuration_file_path: ' + str(configuration_file_path))
    # print('configuration_file_name: ' + str(configuration_file_name))
    if DEBUG:
        print('configuration_file: ' + str(configuration_file))

    print('{0:%Y-%m-%d %H:%M:%S} : Parsing configuration file...'
          .format(datetime.datetime.now()))

    # cfg_parser = ConfigParser.SafeConfigParser()
    # cfg_parser = configparser.SafeConfigParser()
    cfg_parser = configparser.ConfigParser()
    cfg_parser.read(configuration_file)

    mysql_host = cfg_parser.get('server', 'host')
    mysql_port = cfg_parser.getint('server', 'port')
    mysql_user = cfg_parser.get('user', 'user')
    mysql_passwd = cfg_parser.get('user', 'passwd')
    csv_delimiter = cfg_parser.get('csv', 'delimiter')

    if DEBUG:
        print('mysql_host: ' + str(mysql_host))
        print('mysql_port: ' + str(mysql_port))
        print('mysql_user: ' + str(mysql_user))
        print('mysql_passwd: ' + str(mysql_passwd))
        print('csv_delimiter: ' + str(csv_delimiter))

    # sys.exit('Break-Point. Exit')  # TODO(JamesBalcomb): Stopping Being Retarded, Learn to Utilize an IDE;

    print('{0:%Y-%m-%d %H:%M:%S} : Establishing database connection...'
          .format(datetime.datetime.now()))

    # create the connection
    # connection = MySQLdb.connect(host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_passwd)
    # Connect to the database
    # connection = pymysql.connect(
    #     host='localhost',
    #     user='user',
    #     password='passwd',
    #     db='db',
    #     charset='utf8mb4',
    #     cursorclass=pymysql.cursors.DictCursor
    # )
    mysql_connection = pymysql.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        passwd=mysql_passwd
    )

    # get the cursor
    mysql_cursor = mysql_connection.cursor()

    print('{0:%Y-%m-%d %H:%M:%S} : Collecting directory information...'
          .format(datetime.datetime.now()))

    # print('script: sys.argv[0] is', repr(sys.argv[0])
    # path_for_directory_of_csv_files = os.path.dirname(sys.argv[0]) + '/'
    # print('path_for_directory_of_csv_files: ' + path_for_directory_of_csv_files)
    path_for_directory_of_csv_files = os.path.dirname(sys.argv[0]) + '/CSV/'

    if DEBUG:
        print('path_for_directory_of_csv_files: ' + path_for_directory_of_csv_files)

    # TODO(JamesBalcomb): Enable the handling of multiple directories
    # directory_list = filter(os.path.isdir, os.listdir(path_for_directory_of_csv_files))
    directory_list = list_of_directories(path_for_directory_of_csv_files)
    directory_count = len(directory_list)

    if DEBUG:
        print('directory_count: ' + str(directory_count))

    if not directory_list:
        print('{0:%Y-%m-%d %H:%M:%S} : Directory List is empty. Exiting...'
              .format(datetime.datetime.now()))
        sys.exit('Mistakes were made.')
    else:
        print('{0:%Y-%m-%d %H:%M:%S} : Directory List is not empty. Continuing...'
              .format(datetime.datetime.now()))

    if directory_count > 1:
        print('{0:%Y-%m-%d %H:%M:%S} : More than 1 element Directory List. Exiting...'
              .format(datetime.datetime.now()))
        sys.exit('Mistakes were made.')
    else:
        print('{0:%Y-%m-%d %H:%M:%S} : Only 1 element in Directory List. Continuing...'
              .format(datetime.datetime.now()))

    print('{0:%Y-%m-%d %H:%M:%S} : Creating database...'
          .format(datetime.datetime.now()))

    directory_for_schema_name = directory_list[0]

    if DEBUG:
        print('directory_for_schema_name: ', directory_for_schema_name)

    SQL_USE_DATABASE = 'USE %s;' % directory_for_schema_name
    # SQL_CHECK_FOR_DATABASE = 'SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = %s;' % directory_for_schema_name
    SQL_CHECK_FOR_DATABASE = 'SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = %s;'
    SQL_DROP_DATABASE = 'DROP DATABASE %s;' % directory_for_schema_name
    SQL_CREATE_DATABASE = 'CREATE DATABASE %s;' % directory_for_schema_name

    # result_check_for_database = mysql_cursor.execute(SQL_CHECK_FOR_DATABASE)
    result_check_for_database = mysql_cursor.execute(SQL_CHECK_FOR_DATABASE, (directory_for_schema_name,))

    if not result_check_for_database:  # Database Schema does not exist.
        print('Schema does not yet exist. Creating it...')

        # try:
        mysql_cursor.execute(SQL_CREATE_DATABASE)

        #   pymysql.err.OperationalError: (
        #       1044,
        #       "Access denied for user 'PyCsvMysql'@'127.0.0.1' to database 'ipeds_2014_provisional_final'"
        #       )
        #

        # except MySQLdb.Error as e:
        #     conn.rollback()              #rollback transaction here
        #     if e[0]!= ###:
        #     raise

        # except MySQLdb.OperationalError, e:
        #     raise e

        # except MySQLdb.Error, e:
        #     print "Error %d: %s" % (e.args[0], e.args[1])
        #     sys.exit (1)

        # except MySQLdb.Error, e:
        #     sys.stderr.write(“[ERROR] %d: %s\n” % (e.args[0], e.args[1]))
        #     return False

        # finally:
        #     mysql_connection.commit()

        # result_check_for_database = mysql_cursor.execute(SQL_CHECK_FOR_DATABASE)
        result_check_for_database = mysql_cursor.execute(SQL_CHECK_FOR_DATABASE, (directory_for_schema_name,))

        if result_check_for_database:
            print('{0:%Y-%m-%d %H:%M:%S} : Schema created. Continuing...'
                  .format(datetime.datetime.now()))
        else:
            print('{0:%Y-%m-%d %H:%M:%S} : Failed to create schema. Exit'
                  .format(datetime.datetime.now()))
            sys.exit('Mistakes were made.')
    else:
        print('{0:%Y-%m-%d %H:%M:%S} : Schema does already exist. Dropping it...'
              .format(datetime.datetime.now()))

        # TODO(JamesBalcomb): Add check for DROP; I do not recall for what reason, if any, I had not done so.
        mysql_cursor.execute(SQL_DROP_DATABASE)
        # result_drop_database = mysql_cursor.execute(SQL_DROP_DATABASE)
        # if not result_drop_database:
        #    print('Failed to drop schema. Exiting...')
        #    sys.exit('Mistakes were made.')

        # result_check_for_database = mysql_cursor.execute(SQL_CHECK_FOR_DATABASE)
        result_check_for_database = mysql_cursor.execute(SQL_CHECK_FOR_DATABASE, (directory_for_schema_name,))

        if not result_check_for_database:
            print('{0:%Y-%m-%d %H:%M:%S} : Schema dropped. Continuing...'
                  .format(datetime.datetime.now()))
        else:
            print('{0:%Y-%m-%d %H:%M:%S} : Failed to drop schema. Exiting...'
                  .format(datetime.datetime.now()))
            sys.exit('Mistakes were made.')

        mysql_cursor.execute(SQL_CREATE_DATABASE)

        # result_check_for_database = mysql_cursor.execute(SQL_CHECK_FOR_DATABASE)
        result_check_for_database = mysql_cursor.execute(SQL_CHECK_FOR_DATABASE, (directory_for_schema_name,))

        if result_check_for_database:
            print('{0:%Y-%m-%d %H:%M:%S} : Schema re-created. Continuing...'
                  .format(datetime.datetime.now()))
        else:
            print('{0:%Y-%m-%d %H:%M:%S} : Failed to re-create schema. Exit'
                  .format(datetime.datetime.now()))
            sys.exit('Mistakes were made.')

    # close the cursor
    mysql_cursor.close()

    print('{0:%Y-%m-%d %H:%M:%S} : Collecting file information...'
          .format(datetime.datetime.now()))

    # path_for_files = path_for_directory_of_csv_files + directory_for_schema_name + '/'
    path_for_files = path_for_directory_of_csv_files + directory_for_schema_name + '/'

    if DEBUG:
        print('path_for_files: ', path_for_files)

    file_list = []

    for file in os.listdir(path_for_files):
        if fnmatch.fnmatch(file, '*.csv'):
            file_list.append(file)

    if DEBUG:
        print('len(file_list): ', len(file_list))

    print('{0:%Y-%m-%d %H:%M:%S} : Processing file(s) information...'
          .format(datetime.datetime.now()))

    for file_name in file_list:
        table_name = file_name.rstrip('.csv')
        column_lengths = {}
        column_data_types = {}
        count_columns = 0
        set_insert_columns = False
        insert_values_place_holders = ''
        insert_columns = ''
        insert_values_list_item = ''
        insert_values_list = []
        value_is_always_numeric = {}
        value_is_always_integer_number = {}
        value_is_always_unsigned_number = {}
        value_is_rational_number = True
        value_is_integer_number = True
        value_is_negative_number = True
        sql_insert_into_table = ''
        column_index_number_check = 0
        id_unknown_column = 1

        print('{0:%Y-%m-%d %H:%M:%S} : Opening, parsing, and building query for file: {1}'
              .format(datetime.datetime.now(), table_name))

        with open(path_for_files + file_name, 'rt') as fileHandle:

            reader = csv.DictReader(fileHandle, delimiter=csv_delimiter)

            if DEBUG:
                print('len(reader.fieldnames): ', len(reader.fieldnames))

            count_columns = len(reader.fieldnames)

            for index, row in enumerate(reader):
                if index == 0:
                    for heading in row.keys():
                        column_lengths[heading] = 0  # set the dictionary keys
                        column_data_types[heading] = 'UNIDENTIFIED'
                        value_is_always_numeric[heading] = True
                        value_is_always_integer_number[heading] = True
                        value_is_always_unsigned_number[heading] = True
                        # This is because the DictReader returns None when the csv has an extra empty column at the end
                        if heading is None:
                            heading = 'UNKNOWN_COLUMN_' + str(id_unknown_column)
                            id_unknown_column += 1

                for column in row.keys():
                    # This is because the DictReader returns None when the csv has an extra empty column at the end
                    if row[column] is None:
                        # TODO: fix this some day so people can tell the difference between ZLS and NULL
                        row[column] = 'NULL'
                    if len(row[column]) > column_lengths[column]:
                        column_lengths[column] = len(row[column])  # add length as the dictionary value
                    if value_is_always_numeric[column]:
                        if DEBUG_VERBOSE:
                            print('index: ', index)  # this is the index number in dictionary, 0 indexed
                            print('column: ', column)  # this is the key of the csv.DictReader
                            print('value: ', row[column])  # this is the value for the key of the csv.DictReader
                            print('type: ', type(row[column]))
                            print('length: ', len(row[column]))
                        value_is_rational_number = is_float(row[column])
                        if value_is_rational_number:
                            if value_is_always_integer_number:
                                value_is_integer_number = is_integer_number(row[column])
                                if not value_is_integer_number:
                                    value_is_always_integer_number[column] = False
                            if value_is_always_unsigned_number:
                                value_is_unsigned_number = is_unsigned_number(row[column])
                                if not value_is_unsigned_number:
                                    value_is_always_unsigned_number[column] = False
                        else:
                            value_is_always_numeric[column] = False

                    if not value_is_always_numeric[column]:
                        column_data_types[column] = 'varchar' + '(' + str(column_lengths[column]) + ')'

                    if value_is_always_numeric[column]:
                        column_data_types[column] = 'Numeric Type'

                    if value_is_always_numeric[column] \
                            and value_is_always_integer_number[column] \
                            and value_is_always_unsigned_number[column]:
                        column_data_types[column] = 'int(10) unsigned'

                    if value_is_always_numeric[column] \
                            and value_is_always_integer_number[column] \
                            and not value_is_always_unsigned_number[column]:
                        column_data_types[column] = 'int(11) signed'

                    if value_is_always_numeric[column] \
                            and not value_is_always_integer_number[column]:
                        column_data_types[column] = 'decimal(35,15)'  # TODO: stop using 16 bytes by default

                    # Check to see if we started the next row... (hint: index = Row Number)
                    if column_index_number_check != index:
                        if DEBUG:
                            print('##### INDEX CHANGED!! #####')

                        insert_values_list_item = insert_values_list_item.rstrip(',')

                        insert_values_list.append('(' + insert_values_list_item + ')')

                        column_index_number_check = index

                        set_insert_columns = True

                        insert_values_list_item = ''
                        insert_values_list_item += ''' + row[column] + ''' + ','

                    else:
                        if not set_insert_columns:
                            insert_columns += '`' + column.strip() + '`' + ','
                        insert_values_list_item += ''' + row[column] + ''' + ','

            insert_values_list_item = insert_values_list_item.rstrip(',')
            insert_values_list.append('(' + insert_values_list_item + ')')

            insert_columns = insert_columns.rstrip(',')

            insert_values_place_holders = ','.join(['%s'] * count_columns)

            sql_create_table_prefix = 'CREATE TABLE' + ' ' + '`' + table_name + '`' + ' ' + '('
            sql_create_table_middle = ''
            for fieldName in reader.fieldnames:
                sql_create_table_middle += '`' \
                                           + fieldName.strip() \
                                           + '`' \
                                           + ' ' \
                                           + column_data_types[fieldName] \
                                           + ' ' \
                                           + 'DEFAULT NULL' \
                                           + ','

            sql_create_table_suffix = ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'

            sql_create_table = sql_create_table_prefix + sql_create_table_middle.rstrip(',') + sql_create_table_suffix

            sql_check_for_table = 'SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_NAME = %s;'

            sql_drop_table = 'DROP TABLE %s;' % table_name

            # get the cursor
            mysql_cursor = mysql_connection.cursor()
            mysql_cursor.execute(SQL_USE_DATABASE)

            result_check_for_table = mysql_cursor.execute(sql_check_for_table, (table_name,))

            if not result_check_for_table:
                print('{0:%Y-%m-%d %H:%M:%S} : Table does not yet exist. Creating it...'
                      .format(datetime.datetime.now()))
                mysql_cursor.execute(sql_create_table)
                result_check_for_table = mysql_cursor.execute(sql_check_for_table, (table_name,))
                if result_check_for_table:
                    print('{0:%Y-%m-%d %H:%M:%S} : Table created. Continuing...'
                          .format(datetime.datetime.now()))
                else:
                    print('{0:%Y-%m-%d %H:%M:%S} : Failed to create table. Exiting...'
                          .format(datetime.datetime.now()))
                    sys.exit('Mistakes were made.')
            else:
                print('{0:%Y-%m-%d %H:%M:%S} : Table does already exist. Dropping it...'
                      .format(datetime.datetime.now()))
                mysql_cursor.execute(sql_drop_table)
                result_check_for_table = mysql_cursor.execute(sql_check_for_table, (table_name,))
                if not result_check_for_table:
                    print('{0:%Y-%m-%d %H:%M:%S} : Table dropped. Continuing...'
                          .format(datetime.datetime.now()))
                else:
                    print('{0:%Y-%m-%d %H:%M:%S} : Failed to drop table. Exiting...'
                          .format(datetime.datetime.now()))
                    sys.exit('Mistakes were made.')

                mysql_cursor.execute(sql_create_table)

                result_check_for_table = mysql_cursor.execute(sql_check_for_table, (table_name,))

                if result_check_for_table:
                    print('{0:%Y-%m-%d %H:%M:%S} : Table re-created. Continuing...'
                          .format(datetime.datetime.now()))
                else:
                    print('{0:%Y-%m-%d %H:%M:%S} : Failed to re-create table. Exit'
                          .format(datetime.datetime.now()))
                    sys.exit('Mistakes were made.')

            mysql_cursor.close()

    print('{0:%Y-%m-%d %H:%M:%S} : Finished processing files, creating tables, and importing the data.'
          .format(datetime.datetime.now()))

    print('{0:%Y-%m-%d %H:%M:%S} : Ending...'
          .format(datetime.datetime.now()))

if __name__ == '__main__':
    main(sys.argv)
