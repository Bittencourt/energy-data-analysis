#!/usr/bin/env python
# -*- coding: utf8 -*-

from __future__ import division

import pydot
import psycopg2
import sys
from numpy import *
import numpy as np
import time
import matplotlib.pyplot as plt

import skm as skm
from scipy.spatial import distance

from sklearn.cluster import KMeans, DBSCAN
from sklearn.metrics import silhouette_score

from plot_functions import plot_profiles_groups, plotMeanProfileGroups
from fetch_functions import fetch_complete_20_weekdays

from build_functions import buildGridFile

from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import *
from sklearn.decomposition import PCA

import sys

def kmeans_prediction(conn, cursor):

	measures_array = build_mean_measure_array(conn, cursor)
	
	k_means = KMeans(init='k-means++', n_clusters=3, n_init=10)
	t0 = time.time()
	k_means.fit(measures_array)
	t_batch = time.time() - t0
	k_means_labels = k_means.labels_
	k_means_cluster_centers = k_means.cluster_centers_
	k_means_labels_unique = np.unique(k_means_labels)
	
	#print k_means_labels
	for i in k_means_labels:
		print i

def clusterWeekday(conn,cursor,weekday,k,test_id):
	cursor.execute("SELECT contractor_id, quarter, measurement, type FROM \"WeekProfile\" WHERE type != 'Z' and weekday = %d ORDER BY contractor_id, quarter" % (weekday))
	result = cursor.fetchall()
	
	#print result
	
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
	
	#print measurements_array[3,:]
	
	clusters, iterations = skm.skmeans(measurements_array,k,0.00001)
	
	for i in xrange(len(clusters)):
		cursor.execute("INSERT INTO \"ClusterResults\" (contractor_id, test_id, cluster_id) VALUES (%d, %d, %d);" % (contractor_id_list[i], test_id, clusters[i]))
	conn.commit()
		
	
	#print skm.kcluster(measurements_array, k, 100)

	return len(clusters), iterations
	
#end def


def clusterWeekdayMonFri(conn,cursor,k,test_id):
	
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
			
		
		
		
		
		"""for i in result:
			#print i[0]
			if(i[0] != contractor_id and contractor_id != -1): #novo contractor
				#if(sum(contractor_data_array) > 0):
				if(contractor_id not in black_list):
					#print sum(contractor_data_array)
					measurements_array.append(contractor_data_array)
					contractor_id_list.append(contractor_id)
				#end
				contractor_data_array = []
			#end
			contractor_id = i[0]
			contractor_data_array.append(i[2])
		#end
		#if(sum(contractor_data_array) > 0):
		if(contractor_id not in black_list):
			measurements_array.append(contractor_data_array)
			contractor_id_list.append(contractor_id)
		#end"""
		
	
			
		
		
		"""measurements_array=array(measurements_array)"""
		
		#clusters, iterations = skm.skmeans(measurements_array,k,0.00001)
	#end
	"""print black_list"""
	
	clusters, iterations = skm.skmeans(measurements_array,k,0.00001)
	
	for i in xrange(len(clusters)):
		cursor.execute("INSERT INTO \"ClusterResults\" (contractor_id, test_id, cluster_id) VALUES (%d, %d, %d);" % (contractor_id_list[i], test_id, clusters[i]))
	conn.commit()
		
	
	#print skm.kcluster(measurements_array, k, 100)
	
	cursor.execute("UPDATE \"Tests\" SET obs = '%d elements / %d iterations' WHERE test_id = %d" % (len(clusters),iterations,test_id))
	conn.commit()

	return len(clusters), iterations
	
#end def



#Feito no MIT (27/01)
def clusterAllProfiles(conn,cursor,k):
	# Essa função clusteriza todos os profiles dos 644 "escolhidos" nos 15 dias da semana do intervalo
	
	"""# Trabalhando apenas com os 644 "escolhidos"
	cursor.execute("SELECT distinct contractor_id FROM \"ContractorDailyFeatures\" ORDER BY contractor_id")
	contractor_id_list = cursor.fetchall()
	#print "Fez o select 1"
	
	# Trabalhando apenas com os 15 dias de semana dentro dos 21
	cursor.execute("SELECT distinct day_id FROM \"ContractorDailyFeatures\" ORDER BY day_id")
	day_id_list = cursor.fetchall()"""
	
	# Trabalhando apenas com os contractors do grupo grosseiro 0 (em uma divisão por 3 grupos principais) entre os 644 "escolhidos"
	cursor.execute("SELECT contractor_id, day_id, quarter, measurement FROM \"Measurements\" WHERE contractor_id in (SELECT distinct contractor_id FROM \"ContractorsK3\" WHERE group_id = 0) and day_id in (4,5,8,9,10,11,12,15,16,17,18,19,22,23,24) ORDER BY contractor_id, day_id, quarter")
	result = cursor.fetchall()
	
	contractor_id = -1
	day_id = -1
	measurements_array = []
	contractor_day_data_array = [] # guarda a informacao por contractor por dia para depois ser anexado no array maior
	contractor_id_day_id_list = [] # guarda quem é o contractor e o dia de cada linha (como um dicionário)
	for i in result:
		if( (i[0] != contractor_id or i[1] != day_id) and (contractor_id != -1 or day_id != -1)): #novo contractor ou novo dia
			measurements_array.append(contractor_day_data_array)
			contractor_id_day_id_list.append([contractor_id,day_id])
			contractor_day_data_array = []
		#end
		contractor_id = i[0]
		day_id = i[1]
		contractor_day_data_array.append(i[3])
	#end
	measurements_array.append(contractor_day_data_array)
	contractor_id_day_id_list.append([contractor_id,day_id])
	
	measurements_array=array(measurements_array)
	
	for i in xrange(shape(measurements_array)[0]):
		max_value = max(measurements_array[i][:])
		if (max_value > 0):
			for j in xrange(shape(measurements_array)[1]):
				measurements_array[i][j] = float(measurements_array[i][j]) / float(max_value)
	#exit()
	
	#print measurements_array[3,:]
	
	#print shape(measurements_array)
	#print contractor_id_day_id_list
	
	
	#clusters, iterations = skm.skmeans(measurements_array,k,0.00001)
	
	for k in range(2,20):
		print "Clustering... (k =",k,")"	
		k_means = KMeans(init='k-means++', n_clusters=k, n_init=10)
		t0 = time.time()
		k_means.fit(measurements_array)
		t_batch = time.time() - t0
		k_means_labels = k_means.labels_
		k_means_cluster_centers = k_means.cluster_centers_
		k_means_labels_unique = np.unique(k_means_labels)
	
		#for i in k_means_labels:
		#	print i
		
		print "Clusterink OK!"
		#print "Calculating silhouette score..."
		#silhouette_avg = silhouette_score(measurements_array, k_means_labels)
		#print "\tSilhouette (k=",k,")",":",silhouette_avg
	
	
		print "Plotting (k = %d)" % (k)
		# aqui eu ploto tudo e coloco os arquivos nas pastas
		plot_profiles_groups(conn,cursor,measurements_array,contractor_id_day_id_list,k_means_labels,k)
	
	#for i in xrange(len(clusters)):
	#	cursor.execute("INSERT INTO \"ClusterResults\" (contractor_id, test_id, cluster_id) VALUES (%d, %d, %d);" % (contractor_id_list[i], test_id, clusters[i]))
	#conn.commit()
		
	
	#print skm.kcluster(measurements_array, k, 100)

	return len(clusters), iterations
	
#end def

# Feito no MIT (28/01)
def cluster_contractors_daily_load(conn,cursor,k):
# Essa função tem como objetivo clusterizar os usuários com base apenas no consumo diário
#   para definir uma separação grosseira dos contratantes e fazer uma análise mais adequada.
# Não é feita nenhuma normalização nos dados
# Função usa o k-means do sklearn

	# chamando a função para construir a matriz
	measurements_array, contractor_id_list = build_measure_array_selected(conn, cursor)

	#for k in range(2,20):
	#k = 2
	#print "\nClustering... (k =",k,")"	
	k_means = KMeans(init='k-means++', n_clusters=k, n_init=10)
	t0 = time.time()
	k_means.fit(measurements_array)
	t_batch = time.time() - t0
	k_means_labels = k_means.labels_
	k_means_cluster_centers = k_means.cluster_centers_
	k_means_labels_unique = np.unique(k_means_labels)
	
	#for i in xrange(k):
	#	print "\t",size(where(k_means_labels == i))

	#exit()

	#for i in k_means_labels:
	#	print i
	
	#print "Clusterink OK!"
	#print "Calculating silhouette score..."
	silhouette_avg = silhouette_score(measurements_array, k_means_labels)
	#print "\tSilhouette (k=",k,")",":",silhouette_avg
	#print k, silhouette_avg
	#print contractor_id_list
	
	return [contractor_id_list, k_means_labels]

#end


#Feito no MIT (01/02)
def calculateEntropy(conn,cursor):
	# Função bem grande... faz um monte de coisas...
	#
	# Essa função clusteriza todos os profiles dos contractors, com as seguintes restrições:
	# 1) Primeiro pega só os caras que tem pelo menos 20 dias da semana completos
	# 2) Depois pega só os "light users", aqueles que usam menos que 20kwh por dia na média
	#    (o que eu considerei que podem ser os households)
	# Com esses caras, a função clusteriza os profiles (considerei k = 10)
	# Depois calcula a entropia, ordena e pega somente os 80% mais estáveis
	# Retorno:
	#	1) Lista com os usuarios estáveis (que atendem aos critérios colocados anteriormente)
	#	2) Lista com listas dos dias completos dos usuários estáveis
	
	complete_20_weekdays_id_list,complete_20_weekdays_days_list = fetch_complete_20_weekdays(conn,cursor)
	
	# Trabalhando apenas com os contractors que tem um uso leve (menos de 20 kwh por dia)
	# (O trecho de código a seguir pode ser usado sempre que eu quiser separar os caras de uso "leve")
	# (se a definição de "leve" mudar, é só eu rodar isso de novo da forma que eu quiser)
	mean_load_overall = []
	contractor_id_light = []
	contractor_days_light = []
	
	contractor_load_light = []
	for i in xrange(size(complete_20_weekdays_id_list)):
		total_load_contractor = []
		for day_id in complete_20_weekdays_days_list[i]:
			cursor.execute("SELECT total_load FROM \"ContractorDailyFeatures20Days\" where contractor_id = %d and day_id = %d" % (complete_20_weekdays_id_list[i], day_id))
			features_list = cursor.fetchall()
			
			total_load_contractor.append(features_list[0])
		#end
		mean_load_contractor = mean(total_load_contractor)
		###if(mean_load_contractor <= 20):
		contractor_load_light.append(mean_load_contractor)
		contractor_id_light.append(complete_20_weekdays_id_list[i])
		contractor_days_light.append(complete_20_weekdays_days_list[i])
		####end
		
		mean_load_overall.append(mean_load_contractor)
	#end
	
	
	"""
	#Trecho de código que usei para exportar os contratantes em txt
	weekday_profile_all_contractors = []
	for i in xrange(size(contractor_id_light)): # Para cada contratante
		#print contractor_id_light[i]
		str_days = "%s" % (contractor_days_light[i])
		str_days = str_days.replace("[","(")
		str_days = str_days.replace("]",")")
		#print "SELECT measurement, quarter FROM \"Measurements\" where contractor_id = %d and day_id IN %s ORDER BY quarter " % (contractor_id_light_stable[i], str_days)
		
		
		weekday_profile_contractor = []
		for j in range(1,97):
			cursor.execute("SELECT measurement FROM \"Measurements\" where contractor_id = %d and quarter = %d and day_id IN %s ORDER BY quarter " % (contractor_id_light[i], j, str_days))
			resultado = cursor.fetchall()
			
			contractor_quarter = []
			for measurement in resultado:
				contractor_quarter.append(measurement[0])
			#end
			mean_contractor_quarter = mean(contractor_quarter)
			weekday_profile_contractor.append(mean_contractor_quarter)
			#cursor.execute("INSERT INTO \"TypicalWeekdayContractor\" (contractor_id, quarter, measurement) VALUES (%d, %d, %f);" % (contractor_id_light_stable[i], j, mean_contractor_quarter))	
		#end
		weekday_profile_all_contractors.append(weekday_profile_contractor)
		
		#conn.commit()	
	#end
	
	saida = ''
	for i in xrange(size(contractor_id_light)):
		saida =  '%s\n%s' % (saida, contractor_id_light[i])
	
	print saida
	
	exit()
	
	print shape(weekday_profile_all_contractors)
	
	saida = ''
	for i in xrange(shape(weekday_profile_all_contractors)[0]):
		for j in xrange(shape(weekday_profile_all_contractors)[1]):
			saida = '%s\t%.4f' % (saida, weekday_profile_all_contractors[i][j])
		#end
		saida = '%s\n' % (saida)
	#end
	
	print saida
	
	exit()
	"""
	
	print shape(complete_20_weekdays_id_list)
	print complete_20_weekdays_days_list
	print shape(mean_load_overall)
	
	
	# Montando a matriz a ser clusterizada
	measurements_array = []
	contractor_id_day_id_list = [] # guarda quem é o contractor e o dia de cada linha (como um dicionário)
	for i in xrange(size(contractor_id_light)):
		for day_id in contractor_days_light[i]:
			cursor.execute("SELECT contractor_id, day_id, quarter, measurement FROM \"Measurements\" WHERE contractor_id = %d and day_id = %d ORDER BY quarter" % (contractor_id_light[i],day_id))
			result = cursor.fetchall()
			
			contractor_day_data_array = [] # guarda a informacao por contractor por dia para depois ser anexado no array maior
			for j in result:
				contractor_day_data_array.append(j[3])
			#end
			contractor_id_day_id_list.append([contractor_id_light[i],day_id])
			measurements_array.append(contractor_day_data_array)
			
		#end
	#end
	#print measurements_array
	measurements_array=array(measurements_array)
	
	# Normalizando (avaliar se vale a pena)
	for i in xrange(shape(measurements_array)[0]):
		max_value = max(measurements_array[i][:])
		if (max_value > 0):
			for j in xrange(shape(measurements_array)[1]):
				measurements_array[i][j] = float(measurements_array[i][j]) / float(max_value)
				
	
	#print shape(measurements_array)
		
	
	# Agora eu clusterizo (considerei k=10)
	#for k in range(2,20):
	k = 10
	#print "Clustering... (k =",k,")"	
	k_means = KMeans(init='k-means++', n_clusters=k, n_init=10)
	t0 = time.time()
	k_means.fit(measurements_array)
	t_batch = time.time() - t0
	k_means_labels = k_means.labels_
	k_means_cluster_centers = k_means.cluster_centers_
	k_means_labels_unique = np.unique(k_means_labels)

	#for i in k_means_labels:
	#	print i
	
	#print "Clusterink OK!"
	#print "Calculating silhouette score..."
	#silhouette_avg = silhouette_score(measurements_array, k_means_labels)
	#print "\tSilhouette (k=",k,")",":",silhouette_avg

	# aqui eu ploto tudo e coloco os arquivos nas pastas
	#print "Plotting (k = %d)" % (k)
	#plot_profiles_groups(conn,cursor,measurements_array,contractor_id_day_id_list,k_means_labels,k)
	
	#end for
	
	# Descobrindo a entropia
	clusters_contractors = zeros((size(contractor_id_light),k),int)
	distinct_clusters_contractors = zeros((size(contractor_id_light),k),int)
	for i in xrange(shape(measurements_array)[0]): # Para cada um dos registros clusterizados
		pos = contractor_id_light.index(contractor_id_day_id_list[i][0])
		distinct_clusters_contractors[pos][k_means_labels[i]] = 1
		clusters_contractors[pos][k_means_labels[i]] = clusters_contractors[pos][k_means_labels[i]] + 1
		
	
	#for i in xrange(size(contractor_id_light)):
	#	total_days = sum(clusters_contractors[i][:])
	#	sys.stdout.write("%d: \t" % (contractor_id_light[i]) )
	#	for j in xrange(k):
	#		#clusters_contractors[i][j] = float(clusters_contractors[i][j]) / float(total_days)
	#		sys.stdout.write("%.2f \t" % (clusters_contractors[i][j]) )
	#		
	#	sys.stdout.write("\n")
		
	
	# entropy guarda o número de grupos diferentes que 
	entropy = sum(distinct_clusters_contractors, axis=1)
	
	entropy = array(entropy)
	
	# usando o argsort pra guardar os indices do entropy sorteado
	sort_key = np.argsort(entropy)
	
	
	#for i in xrange(size(entropy)):
	#	print contractor_id_light[sort_key[i]], sort_key[i], entropy[sort_key[i]]
		
	#plt.scatter(entropy,contractor_load_light)
	#plt.grid(True)
	
	#plt.xlabel('Variability')
	#plt.ylabel('Load')
	#plt.title(r'Entropy (k = %d)' % k)
	
	## Tweak spacing to prevent clipping of ylabel
	#plt.subplots_adjust(left=0.15)
	#plt.savefig('analysis_results/variability_k%d.png' % k)
	#plt.show()
	
	#plt.clf()

	#exit()
	#####plotEntropy(conn,cursor,entropy)
	
	
	# agora vou pegar os 80% mais estáveis
	contractor_id_light_stable = []
	contractor_days_light_stable = []
	for i in xrange(int(size(entropy) * 0.8)):
		contractor_id_light_stable.append(contractor_id_light[sort_key[i]])
		contractor_days_light_stable.append(contractor_days_light[sort_key[i]])
		
	#print "tamanho do light stable: ", shape(contractor_id_light_stable)

	return contractor_id_light_stable, contractor_days_light_stable

	#for i in xrange(len(clusters)):
	#	cursor.execute("INSERT INTO \"ClusterResults\" (contractor_id, test_id, cluster_id) VALUES (%d, %d, %d);" % (contractor_id_list[i], test_id, clusters[i]))
	#conn.commit()
		
	
	#print skm.kcluster(measurements_array, k, 100)

	#return len(clusters), iterations
	
#end def



def new_euclidean_distances(X, Y=None, Y_norm_squared=None, squared=False):
    return cosine_similarity(X,Y)

# Feito no MIT (03/02)
def clusterWeekday20DaysKmeans(conn,cursor):
	# Essa função clusteriza os profiles de dia da semana típico dos contractors, com restrições (completo-light-stable):
	# 1) Primeiro pega só os caras que tem pelo menos 20 dias da semana completos
	# 2) Depois pega só os "light users", aqueles que usam menos que 20kwh por dia na média
	#    (o que eu considerei que podem ser os households)

	
	cursor.execute("SELECT contractor_id, quarter, measurement FROM \"TypicalWeekdayContractor\" ORDER BY contractor_id, quarter")
	result = cursor.fetchall()
	
	#print result
	#exit()
	
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
	
	# Normalizando
	for i in xrange(shape(measurements_array)[0]):
		max_value = max(measurements_array[i][:])
		if (max_value > 0):
			for j in xrange(shape(measurements_array)[1]):
				measurements_array[i][j] = float(measurements_array[i][j]) / float(max_value)

	
	#for k in range(18,41):
	#	print "Clustering... (k =",k,")"
	#	
	#	clusters, iterations = skm.skmeans(measurements_array,k,0.00001)
	#	plotMeanProfileGroups(measurements_array,clusters,contractor_key,k)
	#end
		
	
	#Trecho para usar DBSCAN
	db = DBSCAN(eps=1.5, min_samples=5, metric='euclidean').fit(measurements_array)
	core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
	core_samples_mask[db.core_sample_indices_] = True
	labels = db.labels_

	# Number of clusters in labels, ignoring noise if present.
	n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
	
	plotMeanProfileGroups(measurements_array,labels,contractor_key,n_clusters_)
	
	"""
	#Trecho para usar k-means
	k = 3
	for k in range(2,41):
		print "Clustering... (k =",k,")"	
		k_means = KMeans(init='k-means++', n_clusters=k, n_init=10)
		t0 = time.time()
		
		# monkey patch (ensure cosine dist function is used)
		k_means.euclidean_distances = new_euclidean_distances 
		
		k_means.fit(measurements_array)
		t_batch = time.time() - t0
		k_means_labels = k_means.labels_
		k_means_cluster_centers = k_means.cluster_centers_
		k_means_labels_unique = np.unique(k_means_labels)

		#for i in k_means_labels:
		#	print i
	
		#print "Clusterink OK!"
		#print "Calculating silhouette score..."
		#silhouette_avg = silhouette_score(measurements_array, k_means_labels)
		#print "\tSilhouette (k=",k,")",":",silhouette_avg
		#print k,"\t",silhouette_avg

		#print "Plotting (k = %d)" % (k)
		# aqui eu ploto tudo e coloco os arquivos nas pastas
		#plot_profiles_groups(conn,cursor,measurements_array,contractor_id_day_id_list,k_means_labels,k)
		
		plotMeanProfileGroups(measurements_array,k_means_labels,contractor_key,k)
		#exit()
	#end for
	
	"""
	
	
	
	#for i in xrange(len(clusters)):
	#	cursor.execute("INSERT INTO \"ClusterResults\" (contractor_id, test_id, cluster_id) VALUES (%d, %d, %d);" % (contractor_id_list[i], test_id, clusters[i]))
	#conn.commit()
		
	
	#print skm.kcluster(measurements_array, k, 100)
	
	
	
	
#end def

# Feito no MIT (03/02)
def fetchWeatherEnergy(conn,cursor):
	contractor_id_light_stable, contractor_days_light_stable = calculateEntropy(conn,cursor)
	
	str_contractor_id_light_stable = "%s" % (contractor_id_light_stable)
	str_contractor_id_light_stable = str_contractor_id_light_stable.replace("[","(")
	str_contractor_id_light_stable = str_contractor_id_light_stable.replace("]",")")
	
	print shape(contractor_id_light_stable)
	
	measurements_series = []
	weather_series = []
	for i in range(62):
		for j in range(97):
			cursor.execute("SELECT day_id, quarter, weather FROM \"Weather\" WHERE day_id = %d AND quarter = %d" % (i,j))
			result = cursor.fetchall()
	
			if(size(result) > 0):
				day_id = result[0][0]
				quarter = result[0][1]
				weather = result[0][2]
				
				cursor.execute("SELECT measurement FROM \"Measurements\" WHERE contractor_id in %s AND day_id = %d AND quarter = %d" % (str_contractor_id_light_stable,i,j))
				result = cursor.fetchall()
				
				measurements_day_quarter = []
				for line in result:
					measurements_day_quarter.append(line[0])
				#end
				mean_measurements_day_quarter = mean(measurements_day_quarter)
				measurements_series.append(mean_measurements_day_quarter)
				weather_series.append(weather)
			#end
		#end
	#end
	print measurements_series
	print weather_series
		
		
	plt.scatter(weather_series,measurements_series)
	plt.grid(True)
	
	plt.xlabel('Weather')
	plt.ylabel('Load')
	plt.title('Weather X Load')
	
	## Tweak spacing to prevent clipping of ylabel
	plt.subplots_adjust(left=0.15)
	plt.savefig('analysis_results/weatherXload.png')
	#plt.show()
	
	plt.clf()
	
#end

#Feito no MIT (09/02)
def clusterWeekdayProfiles(conn,cursor):
	
	cursor.execute("SELECT count(distinct contractor_id) FROM \"ContractorsOverallFeatures\" WHERE num_complete_weekdays >= 20")
	result = cursor.fetchall()
	num_contractors = result[0][0]
	
	# Select utilizando a entropia	
	#cursor.execute("SELECT contractor_id, quarter, measurement FROM \"TypicalWeekdayAllContractors\" WHERE contractor_id IN (SELECT contractor_id from \"ContractorsOverallFeatures\" where num_complete_weekdays >= 20 ORDER BY entropy_k10 limit %d) ORDER BY contractor_id, quarter" % (int(num_contractors * 0.8)))
	
	# Select sem utilizar a entropia
	cursor.execute("SELECT contractor_id, quarter, measurement FROM \"TypicalWeekdayAllContractors\" ORDER BY contractor_id, quarter")
	result = cursor.fetchall()
	
	contractor_id = -1
	measurements_array = []
	contractor_day_data_array = [] # guarda a informacao por contractor por dia para depois ser anexado no array maior
	contractor_key = []
	for i in result:
		if( i[0] != contractor_id and contractor_id != -1 ): #novo contractor
			measurements_array.append(contractor_day_data_array)
			contractor_key.append(contractor_id)
			contractor_day_data_array = []
		#end
		contractor_id = i[0]
		contractor_day_data_array.append(i[2])
	#end
	measurements_array.append(contractor_day_data_array)
	contractor_key.append(contractor_id)
		
	measurements_array=array(measurements_array)
	
	# Normalizando
	# (Divide cada linha pelo seu máximo)
	for i in xrange(shape(measurements_array)[0]):
		max_value = max(measurements_array[i][:])
		if (max_value > 0):
			for j in xrange(shape(measurements_array)[1]):
				measurements_array[i][j] = float(measurements_array[i][j]) / float(max_value)
				
	#for i in range(97):
	pca = PCA(n_components=22)
	pca.fit(transpose(measurements_array))
	PCA(copy=True, whiten=False)
	#print i,sum(pca.explained_variance_ratio_)
	#print sum(pca.explained_variance_ratio_)
	#print shape(pca.components_)
	#exit()
	
	
	#Trecho para usar k-means
	k = 6
	#for k in range(2,41):
	#print "Clustering... (k =",k,")"	
	k_means = KMeans(init='k-means++', n_clusters=k, n_init=10)
	t0 = time.time()
	
	# monkey patch (ensure cosine dist function is used)
	k_means.euclidean_distances = new_euclidean_distances 
	
	k_means.fit(transpose(pca.components_))
	t_batch = time.time() - t0
	k_means_labels = k_means.labels_
	k_means_cluster_centers = k_means.cluster_centers_
	k_means_labels_unique = np.unique(k_means_labels)

	#for i in k_means_labels:
	#	print i

	#print "Clusterink OK!"
	#print "Calculating silhouette score..."
	silhouette_avg = silhouette_score(transpose(pca.components_), k_means_labels)
	#print "\tSilhouette (k=",k,")",":",silhouette_avg
	print k,"\t",silhouette_avg
	
	print shape(k_means_labels)
	#print contractor_key
	
	count = 0
	for i in range(size(contractor_key)):
		#print i, k_means_labels[i],contractor_key[i]
		cursor.execute("UPDATE \"ContractorsOverallFeatures\" SET cluster = %d WHERE contractor_id = %d" % (k_means_labels[i],contractor_key[i]))
		count = count + 1
	conn.commit()
	
	#print "count",count

	plotMeanProfileGroups(measurements_array,k_means_labels,contractor_key,k)
	
	##end for
	
	filename= 'grid_file.csv'
	buildGridFile(conn,cursor,contractor_key,k,k_means_labels,filename)
		
	
#end


#Feito no MIT (09/02)
def clusterWeekdayPremises(conn,cursor):
	
	cursor.execute("SELECT count(distinct contractor_id) FROM \"ContractorsOverallFeatures\" WHERE num_complete_weekdays >= 20")
	result = cursor.fetchall()
	num_contractors = result[0][0]
	
	# Select utilizando a entropia	
	#cursor.execute("SELECT contractor_id, quarter, measurement FROM \"TypicalWeekdayAllContractors\" WHERE contractor_id IN (SELECT contractor_id from \"ContractorsOverallFeatures\" where num_complete_weekdays >= 20 ORDER BY entropy_k10 limit %d) ORDER BY contractor_id, quarter" % (int(num_contractors * 0.8)))
	
	# Select sem utilizar a entropia
	cursor.execute("SELECT contractor_id, quarter, measurement FROM \"TypicalWeekdayAllContractors\" ORDER BY contractor_id, quarter")
	result = cursor.fetchall()
	
	contractor_id = -1
	measurements_array = []
	contractor_day_data_array = [] # guarda a informacao por contractor por dia para depois ser anexado no array maior
	contractor_key = []
	for i in result:
		if( i[0] != contractor_id and contractor_id != -1 ): #novo contractor
			measurements_array.append(contractor_day_data_array)
			contractor_key.append(contractor_id)
			contractor_day_data_array = []
		#end
		contractor_id = i[0]
		contractor_day_data_array.append(i[2])
	#end
	measurements_array.append(contractor_day_data_array)
	contractor_key.append(contractor_id)
		
	measurements_array = array(measurements_array)
	measurements_array_original = np.copy(measurements_array)
	
	# Normalizando
	# (Divide cada linha pelo seu máximo)
	for i in xrange(shape(measurements_array)[0]):
		max_value = max(measurements_array[i][:])
		if (max_value > 0):
			for j in xrange(shape(measurements_array)[1]):
				measurements_array[i][j] = float(measurements_array[i][j]) / float(max_value)
				
				
				
	#for i in range(97):
	pca = PCA(n_components=22)
	pca.fit(transpose(measurements_array))
	PCA(copy=True, whiten=False)
	#print i,sum(pca.explained_variance_ratio_)
	#print sum(pca.explained_variance_ratio_)
	#print shape(pca.components_)
	#exit()
	
	
	#Trecho para usar k-means
	k = 3
	#for k in range(2,41):
	#print "Clustering... (k =",k,")"	
	k_means = KMeans(init='k-means++', n_clusters=k, n_init=10)
	t0 = time.time()
	
	# monkey patch (ensure cosine dist function is used)
	#k_means.euclidean_distances = new_euclidean_distances 
	
	k_means.fit(transpose(pca.components_))
	t_batch = time.time() - t0
	k_means_labels = k_means.labels_
	k_means_cluster_centers = k_means.cluster_centers_
	k_means_labels_unique = np.unique(k_means_labels)

	#for i in k_means_labels:
	#	print i

	#print "Clusterink OK!"
	#print "Calculating silhouette score..."
	silhouette_avg = silhouette_score(transpose(pca.components_), k_means_labels)
	#print "\tSilhouette (k=",k,")",":",silhouette_avg
	print k,"\t",silhouette_avg
	
	print shape(k_means_labels)
	#print contractor_key
	
	count = 0
	for i in range(size(contractor_key)):
		#print i, k_means_labels[i],contractor_key[i]
		cursor.execute("UPDATE \"ContractorsOverallFeatures\" SET type_premise = %d WHERE contractor_id = %d" % (k_means_labels[i],contractor_key[i]))
		count = count + 1
	conn.commit()
	
	#print "count",count

	plotMeanProfileGroups(measurements_array_original,k_means_labels,contractor_key,k)
	
	##end for
	
	filename= 'grid_file_premises.csv'
	buildGridFile(conn,cursor,contractor_key,k,k_means_labels,filename)
	
	#print measurements_array
	#print measurements_array_original
		
	
#end

