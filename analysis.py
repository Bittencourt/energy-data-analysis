#!/usr/bin/env python
# -*- coding: utf8 -*-

import pydot
import psycopg2
import sys
from numpy import *
import numpy as np
import time

#import matplotlib.pyplot as plt
#from operator import itemgetter
#import statsmodels.api as sm # recommended import according to the docs

from db_basics import *
from plot_functions import *
from aux_functions import *
from feed_functions import *
from fetch_functions import *
from cluster_functions import *
from build_functions import *

connected = conect_db()
conn = connected[0]
cursor = connected[1]



#plot_measurement_contractor_day(conn, cursor, 151, 16)
#fetch_contractor_id_list(conn, cursor)
#build_mean_measure_array(conn, cursor)

#kmeans_prediction(conn, cursor)
#
#contractors_list_seq21 = descobreSeq21(conn,cursor)

#print len(contractors_list_seq21)
#print contractors_list_seq21

#calcWeekProfile(conn,cursor)

#plt.figure(figsize=(15,5))
#plot_weekday_profile(conn, cursor, 2, 3)
#plotAllWeekProfile(conn,cursor)


#clusterWeekday(conn,cursor,1,3,1)
#runTestBatch(conn,cursor)

#clusterWeekdayMonFri(conn,cursor,10,72)

#fetch_cluster_results(conn, cursor)


#(conn,cursor,weekday,theta)


#build_similarity_graph(conn,cursor,0,0)

#run_batch_similarity_graph(conn,cursor)

#for theta in [0.0,0.05,0.1,0.15,0.2,0.25,0.3,0.35,0.4,0.45,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.85,0.9,0.95]:
#	build_similarity_graph_MonFri(conn,cursor,7,theta)

#build_modularity_table()


#preenche_dias_completos(conn,cursor)

#verifica_consistencia(conn,cursor)

#preenche_avg_daily_features(conn,cursor)

#fetch_features(conn, cursor)

#build_measure_array_selected(conn, cursor)

#cluster_contractors_daily_load(conn,cursor)

#fetch_features_cluster(conn, cursor,3)

#clusterAllProfiles(conn,cursor,10)

#fetch_complete_days(conn,cursor)

#fetch_complete_20_weekdays(conn,cursor)

#preenche_avg_daily_features_20_weekdays(conn,cursor)

#calculateEntropy(conn,cursor)

#preencheTypicalWeekday(conn,cursor)

#calculateEntropy(conn,cursor)

#clusterWeekday20DaysKmeans(conn,cursor)

#fetchWeatherEnergy(conn,cursor)

#clusterWeekday20DaysKmeans(conn,cursor)

#fetchWeather(conn,cursor)

#fetchAllContractorsByDay(conn,cursor,7)

#clusterWeekday20DaysKmeans(conn,cursor)

#calculateEntropy(conn,cursor)

#feedLocalization(conn,cursor)

#feedContractorsOverallFeatures(conn,cursor)

#calculateEntropy(conn,cursor)

clusterWeekdayProfiles(conn,cursor)

#clusterWeekdayPremises(conn,cursor)

