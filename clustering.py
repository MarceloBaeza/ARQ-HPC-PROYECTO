from __future__ import print_function

import sys
import numpy as np

from time import time, strftime
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.mllib.clustering import GaussianMixture, GaussianMixtureModel

def parse_date(line):
    date = line[0].split('-')
    date = "-".join(date[:2])
    explosion_value = float(line[1])
    return (date, [explosion_value, 1])

def group_days(day1, day2):
    day = [day1[0] + day2[0]]
    count = [day1[1] + day2[1]]
    return day + count

def parse_month(line):
    date = line[0].split('-')
    date = date[1]
    return (date, float(line[1]))

def average_year(lines):
    rdd = lines.map(lambda line: line.split(',')) \
                .map(parse_date) \
                .reduceByKey(group_days) \
                .sortByKey() \
                .map(lambda line: (line[0], line[1][0] / line[1][1]))
    return rdd

def average_month(years):
    rdd = years.map(parse_month) \
                .reduceByKey(lambda month1, month2: month1 + month2) \
                .sortByKey() \
                .map(lambda line: line[1] / 2.0)
    return rdd

def parseDataset(lines):
    rdd = lines.map(lambda line: [float(line)])
    return rdd

def generate_initial_centroids(months, k):
    if k == 12:
        centroids = map(lambda item: [item], months)
        return centroids
    else:
        if k == 4:
            months = [months[11]] + months[:11]
        centroids = []
        offset = 12 / k
        for i in range(0, len(months), offset):
            centroids.append(months[i : i + offset])

        centroids = map(lambda items: sum(items) / float(offset), centroids)
        centroids = map(lambda item: [item], centroids)

        return centroids


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: kmeans <k>(2, 4, 12)", file=sys.stderr)
        exit(-1)

    currTime = strftime("%x") + '-' + strftime("%X")
    currTime = currTime.replace('/', '-')
    currTime = currTime.replace(':', '-')

    sc = SparkContext(appName="KMeans")
    lines = sc.textFile("hdfs://masterNode:9000/user/spark/dataset_observatory/initial_centroids.csv")
    dataset = sc.textFile("hdfs://masterNode:9000/user/spark/dataset_observatory/training_data.csv")

    average_per_year = average_year(lines) # 2014 and 2015
    average_per_month = average_month(average_per_year)
    data = parseDataset(dataset)
    k = int(sys.argv[1])
    initial_centroids = generate_initial_centroids(average_per_month.collect(), k)
    #print(initial_centroids)
    # KMeans
    start = time()
    kmeans_model = KMeans.train(data, k, maxIterations = 100, initialModel = KMeansModel(initial_centroids))
    end = time()
    elapsed_time = end - start
    kmeans_output = [
        "====================== KMeans ====================\n",
        "Final centers: " + str(kmeans_model.clusterCenters),
        "Total Cost: " + str(kmeans_model.computeCost(data)),
        "Value of K: " + str(k),
        "Elapsed time: %0.10f seconds." % elapsed_time
    ]
    #path = "hdfs://masterNode:9000/user/spark/MODELOS-marcelo/KMEANS-2"
    #kmeans_model.save(sc,path)
    # Gauss KMeans
    start = time()
    gauss_model = GaussianMixture.train(data, k, maxIterations = 20)
    end = time()
    elapsed_time = end - start
    gauss_output = ["====================== Gauss KMeans ====================\n"]
    for i in range(k):
        v1 = ("weight = ", gauss_model.weights[i])
        v2 = ("mu = ", gauss_model.gaussians[i].mu)
        v3 = ("sigma = ", gauss_model.gaussians[i].sigma.toArray())
        gauss_output.append((v1,v2,v3))
    tiempo = "Tiempo: " + str(elapsed_time)
    gauss_output.append(tiempo)

    kmeans_info = sc.parallelize(kmeans_output)
    gauss_info = sc.parallelize(gauss_output)
    kmeans_info.saveAsTextFile("hdfs://masterNode:9000/user/spark/output-marcelo/kmeans_(2)-" + currTime)
    gauss_info.saveAsTextFile("hdfs://masterNode:9000/user/spark/output-marcelo/gauss_kmeans_(2)-" + currTime)
    #path = "hdfs://masterNode:9000/user/spark/MODELOS-marcelo/GAUSS-2"
    #gauss_model.save(sc,path)
    sc.stop()
