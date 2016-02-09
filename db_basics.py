#!/usr/bin/env python
# -*- coding: utf8 -*-

import pydot
import psycopg2
import sys
from numpy import *
import numpy as np
import time

import pydot
import psycopg2
import sys

def conect_db():
# Função para conexão com banco de dados

	#Define our connection string
	conn_string = "host='localhost' dbname='energy' user='postgres' password='ga-pedro'"
	# print the connection string we will use to connect
	# print "Connecting to database\n	->%s" % (conn_string)

	try:
		# get a connection, if a connect cannot be made an exception will be raised here
		conn = psycopg2.connect(conn_string)
		# conn.cursor will return a cursor object, you can use this cursor to perform queries
		cursor = conn.cursor()
		print "Connected!\n"
	except:
		# Get the most recent exception
		exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
		# Exit the script and print an error telling what happened.
		sys.exit("Database connection failed!\n ->%s" % (exceptionValue))
	return conn, cursor
	



