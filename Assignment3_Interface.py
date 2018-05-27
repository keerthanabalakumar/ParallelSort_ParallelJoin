#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
##########################################################################################################


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    def arrange(i, SortingColumnName):
        str = "table" + `i`
        c = openconnection.cursor()
        str1 = "tableinter" + `i`
        c.execute("SELECT * FROM %s ORDER BY %s" % (str1, SortingColumnName))
        row = c.fetchall()
        c.execute("CREATE TABLE %s (LIKE %s)" % (str, str1))
        for eachrow in row:
            c.execute("INSERT INTO %s VALUES %s returning*" % (str, eachrow))
        openconnection.commit()

    cursor = openconnection.cursor()
    cursor1 = openconnection.cursor()
    cursor.execute("SELECT MAX(%s) FROM %s" % (SortingColumnName, InputTable))
    maximum = cursor.fetchone()[0]
    cursor.execute("SELECT MIN(%s) FROM %s" % (SortingColumnName, InputTable))
    minimum = cursor.fetchone()[0]
    step = (maximum - minimum) / 5.0
    j = 1
    name = "tableinter"
    count = minimum
    while count < maximum:
        lower = count
        upper = count + step
        if lower < 0:
            lower = 0
        if lower == minimum:
            cursor.execute("SELECT * FROM %s WHERE %s >= %f AND %s <= %f" % (
                InputTable, SortingColumnName, lower, SortingColumnName, upper))
        if lower != minimum:
            cursor.execute("SELECT * FROM %s WHERE %s > %f AND %s <= %f" % (
                InputTable, SortingColumnName, lower, SortingColumnName, upper))
        newname = name + `j`
        row = cursor.fetchall()
        cursor.execute("CREATE TABLE %s (LIKE %s)" % (newname, InputTable))
        for temp in row:
            cursor1.execute("INSERT INTO %s VALUES %s returning*" % (newname, temp))
        count = upper
        j += 1
    openconnection.commit()
    cursor = openconnection.cursor()

    class myThread(threading.Thread):
        def __init__(self, num):
            threading.Thread.__init__(self)
            self.num = num

        def run(self):
            arrange(self.num, SortingColumnName)

    threads = []
    thread1 = myThread(1)
    thread2 = myThread(2)
    thread3 = myThread(3)
    thread4 = myThread(4)
    thread5 = myThread(5)
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    threads.append(thread1)
    threads.append(thread2)
    threads.append(thread3)
    threads.append(thread4)
    threads.append(thread5)
    for temp in threads:
        temp.join()
    cursor.execute("CREATE TABLE IF NOT EXISTS %s (LIKE table1)" % (OutputTable))
    str = "table1"
    cursor.execute("SELECT * FROM %s" % (str))
    row1 = cursor.fetchall()
    str = "table2"
    cursor.execute("SELECT * FROM %s" % (str))
    row2 = cursor.fetchall()
    str = "table3"
    cursor.execute("SELECT * FROM %s" % (str))
    row3 = cursor.fetchall()
    str = "table4"
    cursor.execute("SELECT * FROM %s" % (str))
    row4 = cursor.fetchall()
    str = "table5"
    cursor.execute("SELECT * FROM %s" % (str))
    row5 = cursor.fetchall()
    for temp in row1:
        cursor.execute("INSERT INTO %s VALUES %s returning*" % (OutputTable, temp))
    for temp in row2:
        cursor.execute("INSERT INTO %s VALUES %s returning*" % (OutputTable, temp))
    for temp in row3:
        cursor.execute("INSERT INTO %s VALUES %s returning*" % (OutputTable, temp))
    for temp in row4:
        cursor.execute("INSERT INTO %s VALUES %s returning*" % (OutputTable, temp))
    for temp in row5:
        cursor.execute("INSERT INTO %s VALUES %s returning*" % (OutputTable, temp))
    openconnection.commit()


def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    def arrange(i):
        temp = "tabler" + `i`
        temp2 = "tablem" + `i`
        temp3 = "finaltable" + `i`
        cursor1 = openconnection.cursor()
        if Table1JoinColumn.upper() == Table2JoinColumn.upper():
            cursor1.execute("CREATE TABLE %s AS SELECT * FROM %s innerjoin %s" % (temp3, temp, temp2))
        else:
            cursor1.execute("CREATE TABLE %s AS SELECT * FROM %s INNER JOIN %s on %s.%s=%s.%s" % (
            temp3, temp, temp2, temp, Table1JoinColumn, temp2, Table2JoinColumn))
        openconnection.commit()

    cursor = openconnection.cursor()
    cursor1 = openconnection.cursor()
    cursor.execute("SELECT MAX(%s) FROM %s" % (Table1JoinColumn, InputTable1))
    maxi1 = cursor.fetchone()[0]
    cursor.execute("SELECT MAX(%s) FROM %s" % (Table2JoinColumn, InputTable2))
    maxi2 = cursor.fetchone()[0]
    cursor.execute("SELECT MIN(%s) FROM %s" % (Table1JoinColumn, InputTable1))
    mini1 = cursor.fetchone()[0]
    cursor.execute("SELECT MIN(%s) FROM %s" % (Table2JoinColumn, InputTable2))
    mini2 = cursor.fetchone()[0]
    if maxi1 < maxi2:
        maxi = maxi2
    else:
        maxi = maxi1

    if mini1 > mini2:
        mini = mini2
    else:
        mini = mini1

    rangevalue = (maxi - mini) / 5.0
    i = 0
    tablename = "tabler"
    tablename1 = "tablem"
    count = mini
    while count < maxi:
        lower = count
        upper = count + rangevalue
        if lower < 0:
            lower = 0
        if lower == mini:
            cursor.execute("SELECT * FROM %s WHERE %s >= %f AND %s <= %f" % (
                InputTable1, Table1JoinColumn, lower, Table1JoinColumn, upper))
        if lower != mini:
            cursor.execute("SELECT * FROM %s WHERE %s > %f AND %s <= %f" % (
                InputTable1, Table1JoinColumn, lower, Table1JoinColumn, upper))
        row = cursor.fetchall()
        newname = tablename + `i`
        cursor.execute("CREATE TABLE %s (LIKE %s)" % (newname, InputTable1))
        for row1 in row:
            cursor.execute("INSERT INTO %s VALUES %s returning*" % (newname, row1))
        count = upper
        i += 1
    i = 0
    count = mini
    while count < maxi:
        lower = count
        upper = count + rangevalue
        if lower < 0:
            lower = 0
        if lower == mini:
            cursor.execute("SELECT * FROM %s WHERE %s >= %f AND %s <= %f" % (
                InputTable2, Table2JoinColumn, lower, Table2JoinColumn, upper))
        if lower != mini:
            cursor.execute("SELECT * FROM %s WHERE %s > %f AND %s <= %f" % (
                InputTable2, Table2JoinColumn, lower, Table2JoinColumn, upper))
        row = cursor.fetchall()
        newname1 = tablename1 + `i`
        cursor.execute("CREATE TABLE %s (LIKE %s)" % (newname1, InputTable2))
        for row1 in row:
            cursor.execute("INSERT INTO %s VALUES %s returning*" % (newname1, row1))
        count = upper
        i += 1
    openconnection.commit()
    class myThread(threading.Thread):
        def __init__(self, id):
            threading.Thread.__init__(self)
            self.id = id

        def run(self):
            arrange(self.id)

    threads = []
    thread0 = myThread(0)
    thread1 = myThread(1)
    thread2 = myThread(2)
    thread3 = myThread(3)
    thread4 = myThread(4)
    thread0.start()
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    threads.append(thread0)
    threads.append(thread1)
    threads.append(thread2)
    threads.append(thread3)
    threads.append(thread4)
    for t in threads:
        t.join()
    cursor.execute("CREATE TABLE IF NOT EXISTS %s (LIKE finaltable0)" % (OutputTable))
    str = "finaltable0"
    cursor.execute("SELECT * FROM %s" % (str))
    row1 = cursor.fetchall()
    str = "finaltable1"
    cursor.execute("SELECT * FROM %s" % (str))
    row2 = cursor.fetchall()
    str = "finaltable2"
    cursor.execute("SELECT * FROM %s" % (str))
    row3 = cursor.fetchall()
    str = "finaltable3"
    cursor.execute("SELECT * FROM %s" % (str))
    row4 = cursor.fetchall()
    str = "finaltable4"
    cursor.execute("SELECT * FROM %s" % (str))
    row5 = cursor.fetchall()
    cursor.execute("CREATE TABLE IF NOT EXISTS %s (LIKE finaltable1)" % (OutputTable))
    for temp in row1:
        cursor.execute("INSERT INTO %s VALUES %s returning*" % (OutputTable, temp))
    for temp in row2:
        cursor.execute("INSERT INTO %s VALUES %s returning*" % (OutputTable, temp))
    for temp in row3:
        cursor.execute("INSERT INTO %s VALUES %s returning*" % (OutputTable, temp))
    for temp in row4:
        cursor.execute("INSERT INTO %s VALUES %s returning*" % (OutputTable, temp))
    for temp in row5:
        cursor.execute("INSERT INTO %s VALUES %s returning*" % (OutputTable, temp))
    openconnection.commit()


# Remove this once you are done with implementation
################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment3
	print "Creating Database named as ddsassignment3"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment3 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
        deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
