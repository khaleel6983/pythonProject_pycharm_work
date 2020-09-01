from sys import argv
from pyspark.conf import SparkConf
from pyspark import SparkContext
#from pyspark.sql import SparkSession
sc = SparkContext("local", "pyspark wordcount")
import sys
#fil = sc.textFile("F:/words.txt")
fil = sys.argv[1]
ou = sys.argv[2]
fii = sc.textFile(fil)
fi = fii.flatMap(lambda x: (x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda x,y: (x+y)))
#fil.saveAsTextFile("F:/pyspark_out")
fii.saveAsTextFile(ou)
