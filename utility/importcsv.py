import csv
import sys
import MySQLdb
import argparse

def gethandle(host, user, passwd, db):

	try:

		connection = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)

		cursor = connection.cursor()

	except Exception, exception:

		print str(exception)

		sys.exit(0)

	return connection, cursor

def readcsv(path2csv):

	try:
	
		records = csv.reader(file(path2csv))

		records = [record for record in records]

	except Exception, exception:

		print str(exception)

		sys.exit(0)

	return records

def importcsv(host, user, passwd, db, table, path2csv):

	connection, cursor = gethandle(host, user, passwd, db)

	records = readcsv(path2csv)

	try:

		command = ", ".join(["%s" for i in len(records[0])])

		command = "INSERT INTO %s VALUES (%s)" % (table, command)

		for record in records:

			cursor.execute(command, record)

		connection.commit()

	except Exception, exception:

		print str(exception)

		sys.exit(0)

	finally:

		cursor.close()

		connection.close()

if __name__ == "__main__":

	argument_parser = argparse.ArgumentParser(description="")
	
	argument_parser.add_argument("host", help="")

	argument_parser.add_argument("user", help="")

	argument_parser.add_argument("passwd", help="")

	argument_parser.add_argument("db", help="")

	argument_parser.add_argument("table", help="")

	argument_parser.add_argument("path2csv", help="")

	args = argument_parser.parse_args()

	importcsv(args.host, args.user, args.passwd, args.db, args.table, args.path2csv)
