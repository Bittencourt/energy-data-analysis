#!/usr/bin/env python
# -*- coding: utf8 -*-

import pydot
import psycopg2
import sys
from numpy import *
import numpy as np
import time

def build_mean_measure_array(conn, cursor):
# Função que retorna um array onde cada linha é um usuário e cada coluna é um dia.
# Cada elemento é a média de consumo do dia.
# Usuários que não possuem todos os dias foram desconsiderados e suas linhas no array são nulas (apesar de existirem).
# Essa função é muito ruim, mas é o que vou usar enquanto não resolvo o que fazer com os dados faltantes.
	
	contractor_id_list = fetch_contractor_id_list(conn, cursor)
	empty_day = 0
	#print size(contractor_id_list)
	measures_array = np.zeros((size(contractor_id_list),61))
	
	contractor_id_count = 0
	for contractor_id in contractor_id_list: # Para cada usuário
		
		contractor_days_mean_array = []
		
		cursor.execute("SELECT distinct day FROM \"Measurements\" WHERE contractor_id = %d ORDER BY day" % contractor_id)
		day_list = cursor.fetchall()
		
		if (size(day_list) != 61): # Usuário tem dias vazios... será ignorado
			empty_day = empty_day + 1
			#print "================= Usuários com dia vazio ", empty_day, ": ", size(day_list)
		else: # Usuário tem registro nos 61 dias, será considerado
			
			contractor_day_mean_array = []
			day_count = 0
			for day in day_list: # Para cada um dos dias
				cursor.execute("SELECT quarter, measurement, type, contractor_id FROM \"Measurements\" WHERE contractor_id = %d AND day = '%s' ORDER BY quarter" % (contractor_id, day[0]))
				record = cursor.fetchall()
				#print record
				
				for line in record:
					contractor_day_mean_array.append(line[1])
				contractor_days_mean_array.append(mean(array(contractor_day_mean_array)))
				measures_array[contractor_id_count][day_count] = mean(array(contractor_day_mean_array))
				day_count = day_count + 1
			#end for
			#measures_array[contractor_id_count][].append(contractor_days_mean_array)
		#end else
		contractor_id_count = contractor_id_count + 1
	#end for
	return measures_array
				
#end def


# Feito no MIT (28/01)
def build_measure_array_selected(conn, cursor):
# Função que retorna um array onde cada linha é um usuário e cada coluna é um dia.
# Cada elemento é o consumo do dia.
# Estou usando apenas os 644 "escolhidos" e os 15 dias da semana completos.
# O objetivo é clusterizar esses usuários para, pelo menos, ter uma divisão grosseira dos mesmos e fazer
#  as análises separadamente dentro de cada grupo grosseiro de usuários
	
	
	# Trabalhando apenas com os 644 "escolhidos"
	# Estou pegando direto tudo da ContractorDailyFeatures porque lá só tem so 644 para os 15 dias
	cursor.execute("SELECT contractor_id, day_id, total_load FROM \"ContractorDailyFeatures\" ORDER BY contractor_id, day_id")
	results = cursor.fetchall()
	
	#print results
	
	contractor_id = -1
	contractor_measurements_list = []
	overall_measurements_list = []
	contractor_id_list = []
	for line in results:
		if (contractor_id != line[0] and contractor_id != -1): # mudou o contractor e não é o primeiro
			contractor_id_list.append(contractor_id)
			overall_measurements_list.append(contractor_measurements_list)
			contractor_measurements_list = []
		#end
		contractor_id = line[0]
		contractor_measurements_list.append(line[2])
	#end
	contractor_id_list.append(contractor_id)
	overall_measurements_list.append(contractor_measurements_list)
				
	return array(overall_measurements_list),contractor_id_list
				
#end def


		
		

def calcWeekProfile(conn,cursor):
#Função para calcular o perfil de cada usuário que tem os 21 dias completos e colocar na tabela 
	contractors_list_seq21 = descobreSeq21(conn,cursor)
	
	cursor.execute("SELECT distinct quarter FROM \"Measurements\" ORDER BY quarter")
	quarter_list = cursor.fetchall()
	
	for contractor in contractors_list_seq21:
		print "Inserindo id", contractor
		for quarter in quarter_list:
			
			
			#Seg
			cursor.execute("select quarter,measurement,day_id from \"Measurements\" where contractor_id = %d and quarter = %d and day_id in (8,15,22)" % (contractor, quarter[0]))
			result = cursor.fetchall()
			measurements_list = []
			for line in result:
				measurements_list.append(line[1])
			measurements_list = array(measurements_list)
			avg_measurements = measurements_list.mean()
			# avg_measurements guarda a média do quarter para a segunda. falta apenas INSERIR!!!
			#print "INSERT INTO \"WeekProfile\" (contractor_id, weekday, quarter, measurement, type) VALUES (%d, %d, %d, %f,'%s');" % (contractor, 0, quarter[0], avg_measurements, 'O')
			cursor.execute("INSERT INTO \"WeekProfile\" (contractor_id, weekday, quarter, measurement, type) VALUES (%d, %d, %d, %f,'%s');" % (contractor, 0, quarter[0], avg_measurements, 'O'))
			#exit()
			
			
			#Ter
			cursor.execute("select quarter,measurement,day_id from \"Measurements\" where contractor_id = %d and quarter = %d and day_id in (9,16,23)" % (contractor, quarter[0]))
			result = cursor.fetchall()
			measurements_list = []
			for line in result:
				measurements_list.append(line[1])
			measurements_list = array(measurements_list)
			avg_measurements = measurements_list.mean()
			# avg_measurements guarda a média do quarter para a ter/qua/qui/sex. falta apenas INSERIR!!!
			cursor.execute("INSERT INTO \"WeekProfile\" (contractor_id, weekday, quarter, measurement, type) VALUES (%d, %d, %d, %f,'%s');" % (contractor, 1, quarter[0], avg_measurements, 'O'))
			
			#Qua
			cursor.execute("select quarter,measurement,day_id from \"Measurements\" where contractor_id = %d and quarter = %d and day_id in (10,17,24)" % (contractor, quarter[0]))
			result = cursor.fetchall()
			measurements_list = []
			for line in result:
				measurements_list.append(line[1])
			measurements_list = array(measurements_list)
			avg_measurements = measurements_list.mean()
			# avg_measurements guarda a média do quarter para a ter/qua/qui/sex. falta apenas INSERIR!!!
			cursor.execute("INSERT INTO \"WeekProfile\" (contractor_id, weekday, quarter, measurement, type) VALUES (%d, %d, %d, %f,'%s');" % (contractor, 2, quarter[0], avg_measurements, 'O'))
			
			#Qui
			cursor.execute("select quarter,measurement,day_id from \"Measurements\" where contractor_id = %d and quarter = %d and day_id in (11,18,4)" % (contractor, quarter[0]))
			result = cursor.fetchall()
			measurements_list = []
			for line in result:
				measurements_list.append(line[1])
			measurements_list = array(measurements_list)
			avg_measurements = measurements_list.mean()
			# avg_measurements guarda a média do quarter para a ter/qua/qui/sex. falta apenas INSERIR!!!
			cursor.execute("INSERT INTO \"WeekProfile\" (contractor_id, weekday, quarter, measurement, type) VALUES (%d, %d, %d, %f,'%s');" % (contractor, 3, quarter[0], avg_measurements, 'O'))
			
			#Sex
			cursor.execute("select quarter,measurement,day_id from \"Measurements\" where contractor_id = %d and quarter = %d and day_id in (12,19,5)" % (contractor, quarter[0]))
			result = cursor.fetchall()
			measurements_list = []
			for line in result:
				measurements_list.append(line[1])
			measurements_list = array(measurements_list)
			avg_measurements = measurements_list.mean()
			# avg_measurements guarda a média do quarter para a ter/qua/qui/sex. falta apenas INSERIR!!!
			cursor.execute("INSERT INTO \"WeekProfile\" (contractor_id, weekday, quarter, measurement, type) VALUES (%d, %d, %d, %f,'%s');" % (contractor, 4, quarter[0], avg_measurements, 'O'))
			
			#Sab
			cursor.execute("select quarter,measurement,day_id from \"Measurements\" where contractor_id = %d and quarter = %d and day_id in (6,13,20)" % (contractor, quarter[0]))
			result = cursor.fetchall()
			measurements_list = []
			for line in result:
				measurements_list.append(line[1])
			measurements_list = array(measurements_list)
			avg_measurements = measurements_list.mean()
			# avg_measurements guarda a média do quarter para o sabado. falta apenas INSERIR!!!
			cursor.execute("INSERT INTO \"WeekProfile\" (contractor_id, weekday, quarter, measurement, type) VALUES (%d, %d, %d, %f,'%s');" % (contractor, 5, quarter[0], avg_measurements, 'O'))
			
			#Dom
			cursor.execute("select quarter,measurement,day_id from \"Measurements\" where contractor_id = %d and quarter = %d and day_id in (7,14,21)" % (contractor, quarter[0]))
			result = cursor.fetchall()
			measurements_list = []
			for line in result:
				measurements_list.append(line[1])
			measurements_list = array(measurements_list)
			avg_measurements = measurements_list.mean()
			# avg_measurements guarda a média do quarter para o domingo. falta apenas INSERIR!!!
			cursor.execute("INSERT INTO \"WeekProfile\" (contractor_id, weekday, quarter, measurement, type) VALUES (%d, %d, %d, %f,'%s');" % (contractor, 6, quarter[0], avg_measurements, 'O'))
			
		# end for quarter
		
		conn.commit() # commit depois que termina com um contractor
	# end for contractor
# end def



			
			

	



def build_similarity_graph(conn,cursor,weekday,theta):
	cursor.execute("SELECT contractor_id, quarter, measurement, type FROM \"WeekProfile\" WHERE type != 'Z' and weekday = %d ORDER BY contractor_id, quarter" % (weekday))
	result = cursor.fetchall()
	
	contractor_id = -1
	measurements_array = []
	contractor_data_array = []
	contractor_id_list = []
	for i in result:
		#print i[0]
		if(i[0] != contractor_id and contractor_id != -1): #novo contractor
			measurements_array.append(contractor_data_array)
			contractor_id_list.append(contractor_id)
			contractor_data_array = []
		#end
		contractor_id = i[0]
		contractor_data_array.append(i[2])
	#end
	measurements_array.append(contractor_data_array)
	contractor_id_list.append(contractor_id)
	
	measurements_array=array(measurements_array)
	
	num_contractors = len(measurements_array)
	
	similarity_array = zeros((num_contractors,num_contractors),float)
	
	saida = ''
	
	mapping = []
	
	print '\t\t\t\t',num_contractors
	
	#for i in xrange(num_contractors):
	#	for j in xrange(i,num_contractors):
	#		if(i != j):
	#			sim = 1 - distance.cosine(measurements_array[i],measurements_array[j])
	#			if(sim >= theta):
	#				similarity_array[i][j] = sim
	#				saida = saida + '%d\t%d\t%f\n' % (i+1,j+1,sim)
	#				"""if(contractor_id_list[i] not in mapping):
	#					mapping.append([i,contractor_id_list[i]])
	#				if(contractor_id_list[j] not in mapping):
	#					mapping.append([j,contractor_id_list[j]])"""
	#
	#f = open('similarity_%d_%.2f' % (weekday,theta), 'w')
	#f.write(saida)
	#f.close
	
	#print saida
	#print mapping
	#print similarity_array
	
			
	#print skm.kcluster(measurements_array, k, 100)
	
#end def



def build_similarity_graph_MonFri(conn,cursor,weekday,theta):
	
	measurements_array_all = []
	black_list = [695,877,466,796,419,534,575]
	for weekday in range(0,5):
		#type != 'Z' and
		
		cursor.execute("SELECT distinct(contractor_id) FROM \"WeekProfile\" WHERE contractor_id not in (695,877,466,796,419,534,575) order by contractor_id")
		result = cursor.fetchall()
		
		contractor_id_list = []
		for i in result:
			contractor_id_list.append(i[0])
		#print contractor_id_list
		#exit()
		contractor_id_list=array(contractor_id_list)
		
		cursor.execute("SELECT contractor_id, quarter, measurement, type FROM \"WeekProfile\" WHERE contractor_id not in (695,877,466,796,419,534,575) and weekday = %d ORDER BY contractor_id, quarter" % (weekday))
		result = cursor.fetchall()
	
		contractor_id = -1
		measurements_array = zeros((len(contractor_id_list),96),float)
		
		for line in result:
			#print line
			ind = np.where(contractor_id_list == line[0])
			ind = ind[0][0]
			measurements_array[ind][line[1]-1] = line[2] / 5
			
		
	num_contractors = len(measurements_array)
	
	similarity_array = zeros((num_contractors,num_contractors),float)
	
	print num_contractors
		
	saida = ''
	
	mapping = []
	
	#for i in xrange(num_contractors):
	#	for j in xrange(i,num_contractors):
	#		if(i != j):
	#			sim = 1 - distance.cosine(measurements_array[i],measurements_array[j])
	#			if(sim >= theta):
	#				similarity_array[i][j] = sim
	#				saida = saida + '%d\t%d\t%f\n' % (i+1,j+1,sim)
	#				"""if(contractor_id_list[i] not in mapping):
	#					mapping.append([i,contractor_id_list[i]])
	#				if(contractor_id_list[j] not in mapping):
	#					mapping.append([j,contractor_id_list[j]])"""
	#
	#weekday = 7
	#f = open('similarity_%d_%.2f' % (weekday,theta), 'w')
	#f.write(saida)
	#f.close
	
	#print saida
	#print mapping
	#print similarity_array
	
#end def

def build_modularity_table():
	
	semana = ['Segunda-feira','Terça-feira','Quarta-feira','Quinta-feira','Sexta-feira','Sábado','Domingo','Seg-Sexta']
	saida = ''
	for weekday in [0,1,2,3,4,5,6,7]:
		
		saida = saida + '\n %s &' % semana[weekday]	
		for theta in [0.0,0.05,0.1,0.15,0.2,0.25,0.3,0.35,0.4,0.45,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.85,0.9,0.95]:
			f = open('../../codigos/newman_peso/out/similarity_%d_%.2f_out.txt' % (weekday,theta), 'r')
			array = []
			for line in f:
				array.append(line)
				
			
			if (array != []):
				print weekday, theta, float(array[0].split(':')[1].split(' ')[1]),int(array[1].split(':')[1].split(' ')[1])
				saida = saida + '%.4f / %d &' % (float(array[0].split(':')[1].split(' ')[1]), int(array[1].split(':')[1].split(' ')[1]))
			else:
				print weekday, theta,"---"
				saida = saida + '---&'
			#exit()
			f.close


	print saida
	
def buildGridFile(conn,cursor,contractor_key,k,k_means_labels,filename):
	
	cursor.execute("SELECT localization_id, localization FROM \"Localization\"")
	localization_id_list = cursor.fetchall()
	
	saida = 'Square ID\tcount all'
	for i in range(k):
		saida = '%s\tcluster %d' % (saida, i)
	saida = '%s\tUndefined\n' % (saida)
	
	for line in localization_id_list:
		localization_id = line[0]
		localization = line[1]
		
		saida = '%s%s' % (saida, localization)
		
		cursor.execute("SELECT contractor_id FROM \"ContractorsOverallFeatures\" WHERE localization_id = %d" % localization_id)
		result = cursor.fetchall()
			
		count_all = size(result)
		
		saida = '%s\t%d' % (saida, count_all)
		
		for i in range(k):
			cursor.execute("SELECT count(*) FROM \"ContractorsOverallFeatures\" WHERE localization_id = %d AND cluster = %d" % (localization_id, i))
			result = cursor.fetchall()
			count_cluster = result[0][0]
			saida = '%s\t%d'% (saida, count_cluster)
			
		
		cursor.execute("SELECT count(*) FROM \"ContractorsOverallFeatures\" WHERE localization_id = %d AND cluster = -1" % localization_id)
		result = cursor.fetchall()
		count_cluster = result[0][0]
		saida = '%s\t%d\n'% (saida, count_cluster)
		
		
	f = open(filename, 'w')
	f.write(saida)
	f.close
	
#end
