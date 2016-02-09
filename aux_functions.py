#!/usr/bin/env python
# -*- coding: utf8 -*-

import pydot
import psycopg2
import sys
from numpy import *
import numpy as np
import time

def descobreSeq21(conn,cursor):
#Função para descobrir os usuários que tem janela completa entre os dias 04 e 24
	cursor.execute("SELECT distinct contractor_id FROM \"Measurements\" ORDER BY contractor_id")
	contractor_id_list = cursor.fetchall()
	#print "Fez o select 1"
	
	cursor.execute("SELECT distinct day_id FROM \"Days\" ORDER BY day_id")
	day_id_list = cursor.fetchall()
	#print "Fez o select 2"
	
	cursor.execute("SELECT distinct quarter FROM \"Measurements\" ORDER BY quarter")
	quarter_list = cursor.fetchall()
	#print "Fez o select 3"
	
	#print "Consultas feitas"
	# Para cada contratante
	
	dados_sequencia = []
	contractors_list_seq21 = []
	for contractor_id in contractor_id_list:
		#print "- Calculando para id",contractor_id[0]
		num_dias_completos_sequencia = 0
		sequencia_dias = []
		
		max_num_dias_completos_sequencia = 0
		max_sequencia_dias = []
				
		for day_id in day_id_list:
		#for day_id in xrange(4,25):
			#print day_id
			cursor.execute("SELECT quarter, type FROM \"Measurements\" WHERE contractor_id = %d AND day_id = %d ORDER BY quarter" % (contractor_id[0], day_id[0]) )
			quarter_type_list = cursor.fetchall()
			#print half_type_list
			
			dia_completo = 1		
			for line in quarter_type_list:
				if line[1] != 'O':
					dia_completo = 0
					break
				#end if
			#end for
			if (dia_completo == 1):
				num_dias_completos_sequencia = num_dias_completos_sequencia + 1
				sequencia_dias.append(day_id[0])
			else: # quebrou a sequencia
				if (num_dias_completos_sequencia > max_num_dias_completos_sequencia): # temos uma nova maior sequencia
					max_num_dias_completos_sequencia = num_dias_completos_sequencia
					max_sequencia_dias = sequencia_dias
				#end if
				num_dias_completos_sequencia = 0
				sequencia_dias = []
				
		#end for
		"""if(max_num_dias_completos_sequencia >= 21 and max_sequencia_dias[0] <= 4):
			#print contractor_id[0], max_num_dias_completos_sequencia, max_sequencia_dias
			#cursor.execute("UPDATE \"Measurements30min\" SET seq21 = %s WHERE day = '%s'" % (day[1], day[0]))
			contractors_list_seq21.append(contractor_id[0])"""
			
		
		
		dados_sequencia_contractor = []
		#dados_sequencia_contractor.append(contractor_id[0])
		dados_sequencia_contractor.append(max_num_dias_completos_sequencia)
		#dados_sequencia_contractor.append(max_num_dias_completos_sequencia)
		
		
		
		dados_sequencia.append(dados_sequencia_contractor)
	#end for
	#conn.commit()
	
	print dados_sequencia
	
	return contractors_list_seq21
	
#end def


def plotAllWeekProfile(conn,cursor):

	cursor.execute("SELECT distinct contractor_id FROM \"WeekProfile\" ORDER BY contractor_id")
	contractor_id_list = cursor.fetchall()
	
	cursor.execute("SELECT distinct weekday FROM \"WeekProfile\" ORDER BY weekday")
	weekday_list = cursor.fetchall()
	
	for contractor_id in contractor_id_list:
		for weekday in weekday_list:
			plot_weekday_profile(conn, cursor, contractor_id[0], weekday[0])
		#end for
	#end for
#end def




def runTestBatch(conn,cursor):

	test_id = 1
	for weekday in range(0,7):
		for k in range(2,11):
			print "\nTest:",test_id
			nelements,iterations = clusterWeekday(conn,cursor,weekday,k,test_id);
			cursor.execute("UPDATE \"Tests\" SET obs = '%d elements / %d iterations' WHERE test_id = %d" % (nelements,iterations,test_id))
			conn.commit()
			test_id = test_id+1
			



def run_batch_similarity_graph(conn,cursor):
	for weekday in [0,1,2,3,4,5,6]:
		print 'weekday:',weekday
		for theta in [0.0,0.05,0.1,0.15,0.2,0.25,0.3,0.35,0.4,0.45,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.85,0.9,0.95]:
			 print '\t\ttheta:',theta
			 build_similarity_graph(conn,cursor,weekday,theta)



