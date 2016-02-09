#!/usr/bin/env python
# -*- coding: utf8 -*-

import csv
import pydot
import psycopg2
import sys

import datetime

from numpy import *
import numpy as np

from fetch_functions import fetchDayID

def conect_db():

	#Define our connection string
	conn_string = "host='localhost' dbname='energy' user='postgres' password='123456'"
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


def insert_measurements_from_csv(conn,cursor):
	row_number = 1
	try:
		# Abrindo arquivo e lendo do CSV
		with open('Measurements.csv', 'rb') as csvfile:
			spamreader = csv.reader(csvfile, delimiter=';', quotechar='|')		
			for row in spamreader:
				if(row_number >= 1 and row_number <= 16000000):
				
					# Obtendo e tratando o CSV para insercao no banco
					contractor_id = int(row[0])
					date_csv_format = row[1].split('/')
					date = date_csv_format[2] + '-' + date_csv_format[1] + '-' + date_csv_format[0]
					quarter = int(row[2])
					measurement = float(row[3].replace(',','.'))
					type_measurement = row[4]
					#print contractor_id, date, quarter, measurement, type_measurement
					#print "INSERT INTO \"Measurements\" (contractor_id, day, quarter, measurement, type) VALUES (%d,'%s',%d,%f,'%s');" % (contractor_id, date, quarter, measurement, type_measurement)
						
					# execute our Query
					if(contractor_id != 1001):
						cursor.execute("INSERT INTO \"Measurements\" (contractor_id, day, quarter, measurement, type) VALUES (%d,'%s',%d,%f,'%s');" % (contractor_id, date, quarter, measurement, type_measurement))
				row_number = row_number + 1
			conn.commit()
		
	
	except:
		# Get the most recent exception
		exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
		# Exit the script and print an error telling what happened.
		sys.exit("Database 'insert_measurements_from_csv' failed!\n ->%s (row_number = %d %f %s)" % (exceptionValue, row_number,measurement,type_measurement))
		


def build_measurements30min(conn,cursor):
	
	cursor.execute("SELECT distinct contractor_id FROM \"Measurements\" ORDER BY contractor_id")
	contractor_id_list = cursor.fetchall()
	
	# Fazer a operação em cada um dos contractor_ids
	#for contractor_id in contractor_id_list:
	
def insert_days(conn,cursor):
	cursor.execute("SELECT distinct day FROM \"Measurements\" ORDER BY day")
	day_list = cursor.fetchall()
	
	day_id = 0
	for day in day_list:
		wkday = day[0].weekday()
		print day_id,') ', day[0], wkday
		cursor.execute("INSERT INTO \"Days\" (day_id, day, weekday) VALUES (%d, '%s', %d)" % (day_id, day[0], wkday) )
		
		day_id = day_id + 1
	#end for
	conn.commit()
	
def update_days(conn,cursor):
	cursor.execute("SELECT day, day_id FROM \"Days\" ORDER BY day")
	day_list = cursor.fetchall()
	
	for day in day_list:
		cursor.execute("UPDATE \"Measurements\" SET day_id = %d WHERE day = '%s'" % (day[1], day[0]))
		#print "UPDATE \"Measurements\" SET day_id = %d WHERE day = '%s'" % (day[1], day[0])
	#end for
	conn.commit()

	#UPDATE DEPARTAMENTO SET SALARIO = 1000 WHERE CODIGODEP = 1
	
def insert_missing_data(conn,cursor):
	cursor.execute("SELECT distinct contractor_id FROM \"Measurements\" ORDER BY contractor_id")
	contractor_id_list = cursor.fetchall()
	print "Fez o select 1"
	
	cursor.execute("SELECT distinct day_id FROM \"Days\" ORDER BY day_id")
	day_id_list = cursor.fetchall()
	print "Fez o select 2"
	
	cursor.execute("SELECT distinct quarter FROM \"Measurements\" ORDER BY quarter")
	quarter_list = cursor.fetchall()
	print "Fez o select 3"
	
	print "Consultas feitas"
	# Para cada contratante
	for contractor_id in contractor_id_list:
		print "- Testanto id",contractor_id[0]
		# Para cada dia
		for day_id in day_id_list:
			#print "-- Testando dia",day_id[0]
			# Para cada quarter
			for quarter in quarter_list:
				cursor.execute("SELECT count(*) FROM \"Measurements\" WHERE contractor_id = %d AND day_id = %d AND quarter = %d" % (contractor_id[0], day_id[0], quarter[0]))
				count = cursor.fetchall()
			
				if count[0][0] == 0: # Dado faltante para algum contratante/dia/quarter
					print "\t\t\t ZERO!!"
					cursor.execute("INSERT INTO \"Measurements\" (contractor_id, day_id, quarter, measurement, type) VALUES (%d,'%s',%d,%f,'%s');" % (contractor_id[0], day_id[0], quarter[0], -1, 'U'))
			#end for
		#end for
		conn.commit() # commit depois que termina com um contractor
	#end for
#end def

def insert_Measurement30min(conn,cursor):
	cursor.execute("SELECT distinct contractor_id FROM \"Measurements\" ORDER BY contractor_id")
	contractor_id_list = cursor.fetchall()
	print "Fez o select 1"
	
	cursor.execute("SELECT distinct day_id FROM \"Days\" ORDER BY day_id")
	day_id_list = cursor.fetchall()
	print "Fez o select 2"
	
	cursor.execute("SELECT distinct quarter FROM \"Measurements\" ORDER BY quarter")
	quarter_list = cursor.fetchall()
	print "Fez o select 3"
	
	print "Consultas feitas"
	# Para cada contratante
	for contractor_id in contractor_id_list:
		print "- Calculando para id",contractor_id[0]
		# Para cada dia
		for day_id in day_id_list:
			# Para cada half
			for i in range(1,49):
				cursor.execute("SELECT measurement, type FROM \"Measurements\" WHERE contractor_id = %d AND day_id = %d AND quarter = %d" % (contractor_id[0], day_id[0], (i*2-1)))
				sql_output = cursor.fetchall()[0]
				measurement1 = sql_output[0]
				type1 = sql_output[1]
				
				#print "SELECT measurement, type FROM \"Measurements\" WHERE contractor_id = %d AND day_id = %d AND quarter = %d" % (contractor_id, day_id, (i*2))
				cursor.execute("SELECT measurement, type FROM \"Measurements\" WHERE contractor_id = %d AND day_id = %d AND quarter = %d" % (contractor_id[0], day_id[0], (i*2)))
				sql_output = cursor.fetchall()[0]
				measurement2 = sql_output[0]
				type2 = sql_output[1]
				#print "SELECT measurement, type FROM \"Measurements\" WHERE contractor_id = %d AND day_id = %d AND quarter = %d" % (contractor_id, day_id, (i*2+1))
				
				#print type1, type2
				# Obter o type1 e o measurement1 das consultas
				if(type1 == 'O' and type2 == 'O'):
					measurement30min = (measurement1 + measurement2) / 2
					type30min = 'O'
					#print "entrou no 1"
				elif (type1 == 'O'):
					measurement30min = measurement1
					type30min = 'I' # Ignore the rebuilt (measurement2)
					#print "entrou no 2"
				elif (type2 == 'O'):
					measurement30min = measurement2
					type30min = 'I' # Ignore the rebuilt (measurement1)
					#print "entrou no 3"
				else:
					measurement30min = -1
					type30min = 'U' # type1 != 'O' and type2 != 'O'
					#print "entrou no 4"
				cursor.execute("INSERT INTO \"Measurements30min\" (contractor_id, day_id, half, measurement30min, type) VALUES (%d, %d, %d, %f,'%s');" % (contractor_id[0], day_id[0], i, measurement30min, type30min))
			#end for
		#end for
		conn.commit() # commit depois que termina com um contractor
	#end for
#end def

def update30min(conn,cursor):
	cursor.execute("SELECT distinct contractor_id FROM \"Measurements30min\" ORDER BY contractor_id")
	contractor_id_list = cursor.fetchall()
	print "Fez o select 1"
	
	cursor.execute("SELECT distinct day_id FROM \"Days\" ORDER BY day_id")
	day_id_list = cursor.fetchall()
	print "Fez o select 2"
	
	cursor.execute("SELECT distinct half FROM \"Measurements30min\" ORDER BY half")
	half_list = cursor.fetchall()
	print "Fez o select 3"
	
	for contractor_id in contractor_id_list:
		# Para cada dia
		for day_id in day_id_list:
			#print "-- Testando dia",day_id[0]
			# Para cada quarter
			for half in half_list:
		
				cursor.execute("SELECT measurement30min FROM \"Measurements30min\" WHERE contractor_id = %d and day_id = %d and half = %d " % (contractor_id[0], day_id[0], half[0]))
				measurement30min = cursor.fetchall()
				
				#print measurement30min[0][0]
				#print "UPDATE \"Measurements30min\" SET measurement30min = %f WHERE contractor_id = %d and day_id = %d and half = %d" % (measurement30min[0][0] * 2, contractor_id[0], day_id[0], half[0])
				cursor.execute("UPDATE \"Measurements30min\" SET measurement30min = %f WHERE contractor_id = %d and day_id = %d and half = %d" % (measurement30min[0][0] * 2, contractor_id[0], day_id[0], half[0]) )
		
		
	#conn.commit()
	
	
# Feito no MIT (02/02)
def insert_weather_from_csv(conn,cursor):
	# Abrindo arquivo e lendo do CSV
	
	weather_array = zeros((62,97),float)
	
	#for i in xrange(61):
	#	sys.stdout.write("\n")
	#	for j in xrange(96):
	#		weather_array[i][j] = -1
	
	with open('../bases/weather_observations_torino.csv', 'rb') as csvfile:
		spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
		
		
		
		
		for row in spamreader:
					
			weather = float(row[18])
			# weather já está OK
			
			day = row[1].split(' ')[0]
			# preciso fazer uma busca no banco e ver qual o day_id que tem essa data
			day_id = fetchDayID(conn,cursor,day)
			# day_id está OK
			
			time_utc = row[1].split(' ')[1]
			# tratando a hora
			
			time = time_utc.split('+')[0]
			utc = time_utc.split('+')[1]
			
			utc = int(utc.split(':')[0])
			
			hour = int(time.split(':')[0])
			minute = int(time.split(':')[1])
			second = int(time.split(':')[2])
			
			#print time
			#print utc
			
			#print hour, minute, second
			
			quarter = (hour * 4) + 1 + int(4* (float(minute) / float(60)) )
			
			#exit()
			
			if(weather_array[day_id][quarter] != 1):
				cursor.execute("INSERT INTO \"Weather\" (day_id, quarter, weather, utc, type) VALUES (%d, %d, %f, %d, '%s');" % (day_id, quarter, weather, utc, 'O'))
				conn.commit()
			
			weather_array[day_id][quarter] = 1

		#end for
	#end with
	
	#for i in xrange(61):
	#	sys.stdout.write("\n")
	#	for j in xrange(96):
	#		sys.stdout.write("%.2f \t" % weather_array[i][j] )

#end

def insertLocalizationContractorsFromCSV(conn,cursor):
	with open('../bases/Localization.csv', 'rb') as csvfile:
		spamreader = csv.reader(csvfile, delimiter=';', quotechar='|')
		
		
		for row in spamreader:
			str_localization = "%s" % (row[1])
			#str_localization = str_localization.replace("_","\\_")
			
			#print str_localization
			
			
			cursor.execute("INSERT INTO \"LocalizationContractors\" (contractor_id, localization) VALUES (%d, '%s');" % (int(row[0]),str_localization))
			conn.commit()
			
	cursor.execute("SELECT * FROM \"Localization\"")
	result = cursor.fetchall()
	
	print result
	
#end

	
	
	

	
		
		


connected = conect_db()
conn = connected[0]
cursor = connected[1]


#insert_measurements_from_csv(conn,cursor)

#build_measurements30min(conn,cursor)

#insert_days(conn,cursor)
#insert_missing_data(conn,cursor)

#insert_Measurement30min(conn,cursor)

#update30min(conn,cursor)

#insert_weather_from_csv(conn,cursor)

insertLocalizationFromCSV(conn,cursor)



	
		
