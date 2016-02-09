#!/usr/bin/env python
# -*- coding: utf8 -*-

import pydot
import psycopg2
import sys
from numpy import *
import numpy as np
import time

import matplotlib.pyplot as plt
from operator import itemgetter
import statsmodels.api as sm # recommended import according to the docs


from sklearn.decomposition import PCA

def fetch_contractor(conn, cursor, contractor_id):
# Função para imprimir tudo sobre um determinado usuário
	try:
		# execute our Query
		cursor.execute("SELECT * FROM \"Measurements\" as m WHERE m.contractor_id = %d" % contractor_id)
		
		# retrieve the records from the database
		records = cursor.fetchall()

	except:
		# Get the most recent exception
		exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
		# Exit the script and print an error telling what happened.
		sys.exit("Database 'fetch_contractor_day' failed!\n ->%s" % (exceptionValue))

	print records

	return records
	
def fetch_contractor_day(conn, cursor, contractor_id, day_id):
# Função para imprimir tudo de um usuário em um dia
	try:
		# execute our Query
		cursor.execute("SELECT quarter,measurement FROM \"Measurements\" as m WHERE m.contractor_id = %d AND m.day_id = '%d' ORDER BY quarter" % (contractor_id, day_id))
		
		# retrieve the records from the database
		records = cursor.fetchall()
		
	except:
		# Get the most recent exception
		exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
		# Exit the script and print an error telling what happened.
		sys.exit("Database 'fetch_contractor_day' failed!\n ->%s" % (exceptionValue))

	
	return records
	

def fetch_weekday_profile(conn, cursor, contractor_id, weekday):
# Função para imprimir tudo de um usuário em um dia
	try:
		# execute our Query
		cursor.execute("SELECT * FROM \"WeekProfile\" WHERE contractor_id = %d AND weekday = %d ORDER BY quarter" % (contractor_id, weekday))
		
		# retrieve the records from the database
		records = cursor.fetchall()

	except:
		# Get the most recent exception
		exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
		# Exit the script and print an error telling what happened.
		sys.exit("Database 'fetch_contractor_day' failed!\n ->%s" % (exceptionValue))

	
	return records
	
	
def fetch_cluster_results(conn, cursor):
# Função para imprimir tudo de um usuário em um dia
	cursor.execute("SELECT * FROM \"Tests\" order by test_id")
	records = cursor.fetchall()
	
	semana = ['Segunda-feira','Terça-feira','Quarta-feira','Quinta-feira','Sexta-feira','Sábado','Domingo','Seg-Sexta']
	saida = ''
	
	test_id = 1
	for i in xrange(8):
		saida = '%s %s %s' % (saida,'\\new', semana[i])
		for j in xrange(2,11):
			saida = '%s %s' % (saida, '&')
			for k in range(j):
				cursor.execute("SELECT * FROM \"ClusterResults\" WHERE test_id = %d AND cluster_id = %d" % (test_id,k))
				records = cursor.fetchall()
				print semana[i],test_id,k,len(records)
				saida = '%s %d %s' % (saida, len(records), '|')
			test_id = test_id + 1
			
			
	print saida


def fetch_contractor_id_list(conn, cursor):
# Função que retorna os ids de todos os contratantes
	try:
		# execute our Query
		#cursor.execute("SELECT distinct contractor_id FROM \"Measurements\" as m WHERE m.contractor_id = %d AND m.day = '%s' ORDER BY day, quarter" % (contractor_id, day))
		cursor.execute("SELECT distinct contractor_id FROM \"Measurements\" ORDER BY contractor_id")
		
		# retrieve the records from the database
		records = cursor.fetchall()
		
		contractor_id_list = []
		#i = 0
		for line in records:
			contractor_id_list.append(line[0])
			#i = i + 1
			#if (i == 10): break
			
		#print contractor_id_list
		#print size(contractor_id_list)

	except:
		# Get the most recent exception
		exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
		# Exit the script and print an error telling what happened.
		sys.exit("Database 'fetch_contractor_id_list' failed!\n ->%s" % (exceptionValue))
		
	return contractor_id_list
	

# Feito no MIT (semana 1)
def fetch_features(conn, cursor):

	# Trabalhando apenas com os 644 "escolhidos"
	cursor.execute("SELECT distinct contractor_id FROM \"ContractorDailyFeatures\" ORDER BY contractor_id")
	contractor_id_list = cursor.fetchall()
	#print "Fez o select 1"
	
	# Trabalhando apenas com os 15 dias de semana dentro dos 21
	cursor.execute("SELECT distinct day_id FROM \"ContractorDailyFeatures\" ORDER BY day_id")
	day_id_list = cursor.fetchall()
	#print "Fez o select 2"
	
	
	mean_load_overall = []
	peak_load_overall = []
	base_load_overall = []
	
	for contractor_id in contractor_id_list:
		total_load_contractor = []
		peak_load_contractor = []
		base_load_contractor = []
		for day_id in day_id_list:
			cursor.execute("SELECT total_load, peak_load_normalized, base_load_normalized FROM \"ContractorDailyFeatures\" where contractor_id = %d and day_id = %d" % (contractor_id[0], day_id[0]))
			features_list = cursor.fetchall()
			
			#print features_list[0][0]
			total_load_contractor.append(features_list[0][0])
			peak_load_contractor.append(features_list[0][1])
			base_load_contractor.append(features_list[0][2])
			
		#end
		#print total_load_contractor
		#print mean(total_load_contractor)
		mean_load_contractor = mean(total_load_contractor)
		mean_peak_load_contractor = mean(peak_load_contractor)
		mean_base_load_contractor = mean(base_load_contractor)
		

		# Média de consumo para um contratante calculada
		# (Fazer também o desvio padrão!!)
		mean_load_overall.append(mean_load_contractor)
		peak_load_overall.append(mean_peak_load_contractor)
		base_load_overall.append(mean_base_load_contractor)
		

	#end
	
	# Lista com todas médias de consumos de todos os contratantes
	# Agora já posso plotar!
	plot_features(array(mean_load_overall),array(peak_load_overall),array(base_load_overall))
	
	
	

#end def


# Feito no MIT (28/01)
def fetch_features_cluster(conn, cursor,k):
# Função para buscar as features dos contractors de cada grupo
		
	
	# Descobrindo quem são os contractors que eu quero analisar
	contractor_id_list, k_means_labels = cluster_contractors_daily_load(conn,cursor,k)
	
	for i in range(k):
		
		contractor_id_key_list = where(k_means_labels == i)[0]
		
		print "Group",i,":",size(contractor_id_key_list)
		#exit()
		
		#for contractor_id_key in contractor_id_key_list:
		#	#print "INSERT INTO \"ContractorsK3\" (contractor_id, group_id) VALUES (%d, %d);" % (contractor_id_list[contractor_id_key], i)
		#	cursor.execute("INSERT INTO \"ContractorsK3\" (contractor_id, group_id) VALUES (%d, %d);" % (contractor_id_list[contractor_id_key], i))
		#conn.commit()
	
		
		# Trabalhando apenas com os 15 dias de semana dentro dos 21
		cursor.execute("SELECT distinct day_id FROM \"ContractorDailyFeatures\" ORDER BY day_id")
		day_id_list = cursor.fetchall()
		#print "Fez o select 2"
	
	
		mean_load_overall = []
		peak_load_overall = []
		base_load_overall = []
	
		for contractor_id_key in contractor_id_key_list:
			total_load_contractor = []
			peak_load_contractor = []
			base_load_contractor = []
			for day_id in day_id_list:
				cursor.execute("SELECT total_load, peak_load_normalized, base_load_normalized FROM \"ContractorDailyFeatures\" where contractor_id = %d and day_id = %d" % (contractor_id_list[contractor_id_key], day_id[0]))
				features_list = cursor.fetchall()
				
				#print contractor_id_list[contractor_id_key], features_list
				#exit()
			
				#print features_list[0][0]
				total_load_contractor.append(features_list[0][0])
				peak_load_contractor.append(features_list[0][1])
				base_load_contractor.append(features_list[0][2])
			
			#end
			#print total_load_contractor
			#print mean(total_load_contractor)
			mean_load_contractor = mean(total_load_contractor)
			mean_peak_load_contractor = mean(peak_load_contractor)
			mean_base_load_contractor = mean(base_load_contractor)
		

			# Média de consumo para um contratante calculada
			# (Fazer também o desvio padrão!!)
			mean_load_overall.append(mean_load_contractor)
			peak_load_overall.append(mean_peak_load_contractor)
			base_load_overall.append(mean_base_load_contractor)
		

		#end
	
		# Lista com todas médias de consumos de todos os contratantes
		# Agora já posso plotar!
		plot_features(array(mean_load_overall),array(peak_load_overall),array(base_load_overall),i)
	
	
	

#end def

#Feito no MIT (29/01)
def fetch_complete_days(conn,cursor):
	
	cursor.execute("SELECT distinct contractor_id FROM \"Measurements\" ORDER BY contractor_id")
	contractor_id_list_result = cursor.fetchall()
	
	contractor_id_list = []
	complete_days_list = []
	for contractor_id in contractor_id_list_result:
		complete_days_contractor = []
		cursor.execute("SELECT contractor_id,day_id FROM \"CompleteDays\" WHERE contractor_id = %d AND num_original_quarters = 96 AND day_id IN (SELECT day_id FROM \"Days\" WHERE weekday in (0,1,2,3,4) )" % (contractor_id[0]))
		#cursor.execute("SELECT count(*) FROM \"CompleteDays\" WHERE contractor_id = %d AND num_original_quarters = 96 AND day_id IN (SELECT day_id FROM \"Days\" WHERE weekday in (0,1,2,3,4) )" % (contractor_id[0]))
		#cursor.execute("SELECT count(*) FROM \"CompleteDays\" WHERE contractor_id = %d AND num_original_quarters = 96" % (contractor_id[0]))
		result = cursor.fetchall()
		
		for line in result:
			complete_days_contractor.append(line[1])
		#end
		
		#num_complete_days = result[0][0]
		#print num_complete_days
		
		contractor_id_list.append(contractor_id[0])
		complete_days_list.append(complete_days_contractor)
		#num_complete_days_list.append(num_complete_days)
	#end
		
	#print num_complete_days_list
	#plot_num_complete_days(contractor_id_list,num_complete_days_list)
	
	#print complete_days_list
	return contractor_id_list, complete_days_list
#end

#Feito no MIT (29/01)
def plot_num_complete_days(contractor_id_list,num_complete_days_list):
	print num_complete_days_list
	
	num_bins = 44
	# Mean Load
	#print "mean load overall:", mean_load_overall
	n, bins, patches = plt.hist(num_complete_days_list, num_bins, normed=True, facecolor='blue', alpha=0.5)
	#print n
	#print "======================="
	# add a 'best fit' line
	#y = mlab.normpdf(bins, mu, sigma)
	#plt.plot(bins, y, 'r--')
	plt.xlabel('Number of complete weekdays')
	plt.ylabel('Probability')
	plt.title(r'Number of complete days (Histogram)')

	# Tweak spacing to prevent clipping of ylabel
	plt.subplots_adjust(left=0.15)
	plt.savefig('analysis_results/num_complete_weekdays_histogram.png')
	#plt.show()
	plt.clf()
	
	
	
	ecdf = sm.distributions.ECDF(num_complete_days_list)
	x = np.linspace(min(num_complete_days_list), max(num_complete_days_list),10000)
	y = ecdf(x)
	plt.step(x, y, linewidth=2)
	
	plt.xlabel('Number of complete weekdays')
	plt.ylabel('Probability (< x)')
	plt.title(r'Number of complete weekdays (CDF)')
	
	# Tweak spacing to prevent clipping of ylabel
	plt.subplots_adjust(left=0.15)
	plt.savefig('analysis_results/num_complete_weekdays_cdf.png')
	plt.show()
	
	plt.clf()
#end

# Feito no MIT (01/02)
def fetch_complete_20_weekdays(conn,cursor):
	contractor_id_list, complete_days_list = fetch_complete_days(conn,cursor)
	
	complete_20_weekdays_id_list = []
	complete_20_weekdays_days_list = []
	for i in xrange(size(contractor_id_list)):
		#print contractor_id_list[i], size(complete_days_list[i])
		if(size(complete_days_list[i]) >= 20):
			complete_20_weekdays_id_list.append(contractor_id_list[i])
			complete_20_weekdays_days_list.append(complete_days_list[i])
	#end
	#print complete_20_weekdays_id_list
	#print size(complete_20_weekdays_id_list)
	
	return complete_20_weekdays_id_list,complete_20_weekdays_days_list
#end def

def fetchDayID(conn,cursor,day):
	cursor.execute("SELECT day_id FROM \"Days\" WHERE day = \'%s\'" % day)
	result = cursor.fetchall()
	
	return result[0][0]
	
#Feito no MIT (05/02)
def fetchWeather(conn,cursor):
	
	weather_list = []
	
	min_temp = zeros((61),float)
	max_temp = zeros((61),float)
	avg_temp = zeros((61),float)
	
	for i in range(61):
		cursor.execute("SELECT day_id, quarter, weather FROM \"Weather\" WHERE day_id = %d ORDER BY day_id, quarter" % i)
		result = cursor.fetchall()
		
		weather_list = []
		for line in result:
			weather_list.append(line[2])
		#print weather_list
		
		#print i, result
		min_temp[i] = min(weather_list)
		max_temp[i] = max(weather_list)
		avg_temp[i] = mean(weather_list)
	#end
	
	saida = 'day\tmin\tmax\tavg\n'
	for i in range(61):
		saida = '%s%d\t%d\t%d\t%d\n' % (saida, i, min_temp[i], max_temp[i], avg_temp[i])
	
	print saida
	#print min_temp
	#print max_temp
	#print avg_temp
	
	
#end


#Feito no MIT (05/02)
def fetchAllContractorsByDay(conn,cursor,day_id):
	
	cursor.execute("SELECT distinct contractor_id FROM \"Measurements\" ORDER BY contractor_id")
	contractor_id_list_result = cursor.fetchall()
	
	contractor_id_list = []
	for contractor_id in contractor_id_list_result:
		cursor.execute("SELECT contractor_id FROM \"CompleteDays\" WHERE contractor_id IN (SELECT contractor_id FROM \"CompleteDays\" WHERE contractor_id = %d AND num_original_quarters = 96 AND day_id = %d) AND num_original_quarters = 96 AND day_id = %d" % (contractor_id[0], 30, 7))
		result = cursor.fetchall()
		
		if(size(result)>0):
			contractor_id_list.append(contractor_id[0])
	#end
	
	#print size(contractor_id_list)
	#exit()
	
	profiles_all_contractors = []
	for contractor_id in contractor_id_list:	
		profile_contractor = []
		for j in range(1,97):
			cursor.execute("SELECT measurement, type FROM \"Measurements\" where contractor_id = %d and quarter = %d and day_id = %d ORDER BY quarter " % (contractor_id, j, 7))
			resultado = cursor.fetchall()
			
			profile_contractor.append(resultado[0][0])
		#end
		profiles_all_contractors.append(profile_contractor)
	#end
	
	#exit()
	
	#print profiles_all_contractors
	
	#exit()
	mean_profile = zeros(96,float)
	for i in range(96):
		mean_profile[i] = mean(profiles_all_contractors[:][i])
				
		
	plotSpecificDay(mean_profile,7)
	
	
	
	
	profiles_all_contractors = []
	for contractor_id in contractor_id_list:	
		profile_contractor = []
		for j in range(1,97):
			cursor.execute("SELECT measurement, type FROM \"Measurements\" where contractor_id = %d and quarter = %d and day_id = %d ORDER BY quarter " % (contractor_id, j, 30))
			resultado = cursor.fetchall()
			
			profile_contractor.append(resultado[0][0])
		#end
		profiles_all_contractors.append(profile_contractor)
	#end
	
	#exit()
	
	#print profiles_all_contractors
	
	#exit()
	mean_profile = zeros(96,float)
	for i in range(96):
		mean_profile[i] = mean(profiles_all_contractors[:][i])
			
	plotSpecificDay(mean_profile,30)
	exit()
	
	

		
	print contractor_id_list
	return contractor_id_list
#end

	



# Feito no MIT (05/02)
def plotSpecificDay(measurement_series,day_id):

	#print measurement_array_groups_mean
	#print quarter_series
	fig = plt.figure(figsize=(15,5))
	quarter_series = range(1,97)
	
	
	lines = plt.plot(quarter_series,measurement_series)
	plt.setp(lines, 'color', 'r', 'linewidth', 4.0)
	
	plt.xlabel('Time')
	plt.ylabel('Consumption (Kwh)')
	
	plt.suptitle('Day %d\n ' % (day_id), fontsize=16)
	
	#plt.ylim([0,max(measurement_series) + 0.1])
	plt.ylim([0,0.7])
	ax = plt.gca()
	ax.yaxis.grid(True)
	plt.locator_params(nbins=20)
	
	peak = np.max(measurement_series)
	peak_ind = np.argmax(measurement_series)
	

	quarter_labels = ['0h','','','','1h','','','','2h','','','','3h','','','','4h','','','','5h','','','','6h','','','','7h','','','','8h','','','','9h','','','','10h','','','','11h','','','','12h','','','','13h','','','','14h','','','','15h','','','','16h','','','','17h','','','','18h','','','','19h','','','','20h','','','','21h','','','','22h','','','','23h','','','']
	plt.xticks(quarter_series,quarter_labels)
	
	"""if (peak_ind < 72):
		plt.annotate('peak (%f Kwh)' % peak, xy=(quarter_series[peak_ind], measurement_array_groups_mean[peak_ind]), xytext=(quarter_series[peak_ind] + 10, measurement_series[peak_ind] + 0.01), arrowprops=dict(facecolor='black', shrink=0.05), )
	else:
		plt.annotate('peak (%f Kwh)' % peak, xy=(quarter_series[peak_ind], measurement_array_groups_mean[peak_ind]), xytext=(quarter_series[peak_ind] - 20, measurement_series[peak_ind] + 0.01), arrowprops=dict(facecolor='black', shrink=0.05), )
		"""
	plt.savefig('analysis_results/profile_day%d.png' % (day_id))
	
	#plt.savefig(filename)
	
	fig.clf()
	plt.clf()
	plt.close(fig)
	#plt.show()
	
#end def


#Feito no MIT (08/02)
def fetchTypicalWeeddayAllContractors(conn,cursor):

	cursor.execute("SELECT contractor_id, quarter, measurement FROM \"TypicalWeekdayAllContractors\" ORDER BY contractor_id, quarter")
	result = cursor.fetchall()
	
	contractor_id = -1
	measurements_array = []
	contractor_day_data_array = [] # guarda a informacao por contractor por dia para depois ser anexado no array maior
	contractor_key = []
	for i in result:
		if( i[0] != contractor_id and contractor_id != -1 ): #novo contractor
			measurements_array.append(contractor_day_data_array)
			contractor_key.append(i[0])
			contractor_day_data_array = []
		#end
		contractor_id = i[0]
		contractor_day_data_array.append(i[2])
	#end
	measurements_array.append(contractor_day_data_array)
	contractor_key.append(i[0])
		
	measurements_array=array(measurements_array)
	
	"""
	saida = ''
	for i in xrange(shape(measurements_array)[0]):
		for j in xrange(shape(measurements_array)[1]):
			saida = '%s\t%.4f' % (saida, measurements_array[i][j])
		#end
		saida = '%s\n' % (saida)
	#end
	
	print saida
	exit()
	"""
	
	
	#print(measurements_array)
	
	#exit()
	
	# Normalizando
	"""for i in xrange(shape(measurements_array)[0]):
		max_value = max(measurements_array[i][:])
		if (max_value > 0):
			for j in xrange(shape(measurements_array)[1]):
				measurements_array[i][j] = float(measurements_array[i][j]) / float(max_value)"""
	
	#measurements_array = transpose(measurements_array)
	pca = PCA(n_components=6)
	pca.fit(measurements_array)
	PCA(copy=True, n_components=6, whiten=False)
	print(pca.explained_variance_ratio_)
	print sum(pca.explained_variance_ratio_)
	print shape(pca.components_)
	exit()

#end
