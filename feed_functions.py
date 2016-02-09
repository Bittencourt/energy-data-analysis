#!/usr/bin/env python
# -*- coding: utf8 -*-

import pydot
import psycopg2
import sys
from numpy import *
import numpy as np
import time

from fetch_functions import fetch_complete_20_weekdays
from operator import itemgetter
import statsmodels.api as sm # recommended import according to the docs

from cluster_functions import calculateEntropy

###### Nova fase (no MIT)
def preenche_dias_completos(conn,cursor):
#Função para descobrir quantos dias completos cada usuário tem
	cursor.execute("SELECT distinct contractor_id FROM \"Measurements\" ORDER BY contractor_id")
	contractor_id_list = cursor.fetchall()
	#print "Fez o select 1"
	
	cursor.execute("SELECT distinct day_id FROM \"Days\" ORDER BY day_id")
	day_id_list = cursor.fetchall()
	#print "Fez o select 2"
	
	cursor.execute("SELECT distinct quarter FROM \"Measurements\" ORDER BY quarter")
	quarter_list = cursor.fetchall()
	#print "Fez o select 3"
	
	print "Consultas feitas"
	
	
	# Para cada contratante
	
	dados_sequencia = []
	#contractors_num_dias_completos = []
	#contractors_dias_completos = []
	for contractor_id in contractor_id_list:
		print "- Calculando para id", contractor_id[0]
		num_dias_completos = 0
		contractors_dias_completos = []
		dias_completos = []
		
		for day_id in day_id_list:
			num_original_quarters = 0 # numero de quarters originais em um dia (ainda não sei...)
			
			#print "\tAnalisando dia",day_id[0],":"
					
			cursor.execute("SELECT quarter, type FROM \"Measurements\" WHERE contractor_id = %d AND day_id = %d ORDER BY quarter" % (contractor_id[0], day_id[0]) )
			quarter_type_list = cursor.fetchall()
			
			#print "\t\t",quarter_type_list
			
			for line in quarter_type_list:
				if line[1] == 'O':
					num_original_quarters = num_original_quarters + 1 # mais um quarto é original
				#end if
			#end for
			
			# Hora de inserir o número de quarters originais
			cursor.execute("INSERT INTO \"CompleteDays\" (contractor_id, day_id, num_original_quarters) VALUES (%d, %d, %d);" % (contractor_id[0], day_id[0], num_original_quarters))
			
			if (num_original_quarters == 96): # todos os quarters são originais
				num_dias_completos = num_dias_completos + 1
				dias_completos.append(day_id[0])
				#print "\t\tDia",day_id[0],"é completo..."
				
			
		#end for (day)

		conn.commit() # commit depois que termina com um contractor
		
		#contractors_num_dias_completos.append(num_dias_completos)
		#contractors_dias_completos.append(dias_completos)
		
		#print "\n\t# dias completos:",num_dias_completos
		#print "\tDias completos:",dias_completos
		#print "\t ============ "
		
	#end for (contractors)
	#conn.commit()
	
	return []

#end def


###### Nova fase (no MIT)
# Feito no MIT (semana 2)
def preenche_avg_daily_features(conn,cursor):
	#Função para preencher a tabela ContractorDailyFeatures

	# Trabalhando apenas com os 644 "escolhidos"
	cursor.execute("SELECT distinct contractor_id FROM \"WeekProfile\" ORDER BY contractor_id")
	contractor_id_list = cursor.fetchall()
	#print "Fez o select 1"
	
	#Trabalhando apenas com os dias de semana no intervalo que está OK
	day_id_list = [4,5,8,9,10,11,12,15,16,17,18,19,22,23,24]
	#print "Fez o select 2"
	
	cursor.execute("SELECT distinct quarter FROM \"Measurements\" ORDER BY quarter")
	quarter_list = cursor.fetchall()
	#print "Fez o select 3"
	
	print "Consultas feitas"
	
	
	# Para cada contratante
	
		
	for contractor_id in contractor_id_list:
		print "- Calculando id", contractor_id[0]
		
		for day_id in day_id_list:
			all_day_load = []
			for quarter_id in quarter_list:
				cursor.execute("SELECT measurement FROM \"Measurements\" where contractor_id = %d and day_id = %d and quarter = %d" % (contractor_id[0], day_id, quarter_id[0]))
				resultado = cursor.fetchall()
				
				all_day_load.append(resultado[0][0])
			#end quarter
			
			total_load_day = sum(all_day_load) # total de um contractor em um dia
			
			# DEIXA O PEAK PRA DAQUI A POUCO!!
			peak_timing, peak_load = max(enumerate(all_day_load), key=itemgetter(1))
			
			if(total_load_day > 0):
				peak_load_normalized = peak_load / total_load_day # peak load normalizado pelo consumo total para um contractor em um determinado dia
			
				base_load_normalized = (min(all_day_load) * 96) / total_load_day # base load normalizado pelo consumo total para um contractor em um determinado dia
			else:
				peak_load_normalized = 0
				base_load_normalized = 0
				peak_timing = 0
			
			# Já tenho as informações do dia de um contractor. Já posso inserir...
			cursor.execute("INSERT INTO \"ContractorDailyFeatures\" (contractor_id, day_id, total_load, peak_load_normalized, base_load_normalized, peak_timing) VALUES (%d, %d, %f, %f, %f, %d);" % (contractor_id[0], day_id, total_load_day, peak_load_normalized, base_load_normalized, peak_timing))
			
			
		#end day
		conn.commit()
	#end contractor
	

# Feito no MIT (01/02)
def preenche_avg_daily_features_20_weekdays(conn,cursor):
	#Função para preencher a tabela ContractorDailyFeatures
	#A informação será armazenada para todos os contratantes que tem mais que 20 weekdays completos

	# Descobrindo os contratantes que têm mais que 20 dias de semana preenchidos
	complete_20_weekdays_id_list,complete_20_weekdays_days_list = fetch_complete_20_weekdays(conn,cursor)
	
	cursor.execute("SELECT distinct quarter FROM \"Measurements\" ORDER BY quarter")
	quarter_list = cursor.fetchall()
	#print "Fez o select 3"
	
	print "Consultas feitas"
	
	
	# Para cada contratante
	
		
	for i in xrange(size(complete_20_weekdays_id_list)):
		print "- Calculando id", complete_20_weekdays_id_list[i]
		
		for day_id in complete_20_weekdays_days_list[i]:
			all_day_load = []
			for quarter_id in quarter_list:
				cursor.execute("SELECT measurement FROM \"Measurements\" where contractor_id = %d and day_id = %d and quarter = %d" % (complete_20_weekdays_id_list[i], day_id, quarter_id[0]))
				resultado = cursor.fetchall()
				
				all_day_load.append(resultado[0][0])
			#end quarter
			
			total_load_day = sum(all_day_load) # total de um contractor em um dia
			
			# DEIXA O PEAK PRA DAQUI A POUCO!!
			peak_timing, peak_load = max(enumerate(all_day_load), key=itemgetter(1))
			
			if(total_load_day > 0):
				peak_load_normalized = peak_load / total_load_day # peak load normalizado pelo consumo total para um contractor em um determinado dia
			
				base_load_normalized = (min(all_day_load) * 96) / total_load_day # base load normalizado pelo consumo total para um contractor em um determinado dia
			else:
				peak_load_normalized = 0
				base_load_normalized = 0
				peak_timing = 0
			
			# Já tenho as informações do dia de um contractor. Já posso inserir...
			cursor.execute("INSERT INTO \"ContractorDailyFeatures20Days\" (contractor_id, day_id, total_load, peak_load_normalized, base_load_normalized, peak_timing) VALUES (%d, %d, %f, %f, %f, %d);" % (complete_20_weekdays_id_list[i], day_id, total_load_day, peak_load_normalized, base_load_normalized, peak_timing))
			
			
		#end day
		conn.commit()
	#end contractor


#end def

# Feito no MIT (02/02)
def preencheTypicalWeekday(conn,cursor):
	# Essa função preenche a tabela de uso típico de um usuário no dia de semana
	
	# Buscando somente os contratantes leves de menor entropia
	contractor_id_light_stable, contractor_days_light_stable = calculateEntropy(conn,cursor)
	
	print "Light stable contracotrs calculated"
	
	weekday_profile_all_contractors = []
	for i in xrange(size(contractor_id_light_stable)): # Para cada contratante
		print contractor_id_light_stable[i]
		str_days = "%s" % (contractor_days_light_stable[i])
		str_days = str_days.replace("[","(")
		str_days = str_days.replace("]",")")
		#print "SELECT measurement, quarter FROM \"Measurements\" where contractor_id = %d and day_id IN %s ORDER BY quarter " % (contractor_id_light_stable[i], str_days)
		
		
		weekday_profile_contractor = []
		for j in range(1,97):
			cursor.execute("SELECT measurement FROM \"Measurements\" where contractor_id = %d and quarter = %d and day_id IN %s ORDER BY quarter " % (contractor_id_light_stable[i], j, str_days))
			resultado = cursor.fetchall()
			
			contractor_quarter = []
			for measurement in resultado:
				contractor_quarter.append(measurement[0])
			#end
			mean_contractor_quarter = mean(contractor_quarter)
			weekday_profile_contractor.append(mean_contractor_quarter)
			cursor.execute("INSERT INTO \"TypicalWeekdayContractor\" (contractor_id, quarter, measurement) VALUES (%d, %d, %f);" % (contractor_id_light_stable[i], j, mean_contractor_quarter))	
		#end
		weekday_profile_all_contractors.append(weekday_profile_contractor)
		
		conn.commit()	
	#end
	#print weekday_profile_all_contractors
	

#end

#Feito no MIT (09/02)
def feedLocalization(conn,cursor):
	cursor.execute("SELECT distinct localization FROM \"LocalizationContractors\"")
	resultado = cursor.fetchall()
	
	localization_id = 0
	for line in resultado:
		localization = line[0]
		cursor.execute("INSERT INTO \"Localization\" (localization_id, localization) VALUES (%d, '%s');" % (localization_id, localization))
		localization_id = localization_id + 1
	conn.commit()	
	

#end


#Feito no MIT (09/02)
def feedContractorsOverallFeatures(conn,cursor):
	cursor.execute("SELECT distinct contractor_id FROM \"Measurements\" ORDER BY contractor_id")
	contractor_id_list = cursor.fetchall()
	
	for contractor_id in contractor_id_list:
		contractor_id = contractor_id[0]
		
		complete_weekdays_contractor = []
		cursor.execute("SELECT count(*) FROM \"CompleteDays\" WHERE contractor_id = %d AND num_original_quarters = 96 AND day_id IN (SELECT day_id FROM \"Days\" WHERE weekday in (0,1,2,3,4) )" % (contractor_id))
		result = cursor.fetchall()
		num_complete_weekdays = result[0][0]
		
		complete_days_contractor = []
		cursor.execute("SELECT count(*) FROM \"CompleteDays\" WHERE contractor_id = %d AND num_original_quarters = 96" % (contractor_id))
		result = cursor.fetchall()
		num_complete_days = result[0][0]
		
		cursor.execute("SELECT localization FROM \"LocalizationContractors\" WHERE contractor_id = %d" % (contractor_id))
		result = cursor.fetchall()
		localization = result[0][0]
		
		cursor.execute("SELECT localization_id FROM \"Localization\" WHERE localization = '%s'" % (localization))
		result = cursor.fetchall()
		localization_id = result[0][0]
		
		cursor.execute("INSERT INTO \"ContractorsOverallFeatures\" (contractor_id,num_complete_weekdays,num_complete_days,localization_id,entropy_k10, type_premise) VALUES (%d, %d, %d, %d, %d, %d);" % (contractor_id, num_complete_weekdays, num_complete_days, localization_id, -1, -1))
		
	conn.commit()	
		
	
	
#end
