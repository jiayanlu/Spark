# Databricks notebook source
inputRDD = sc.textFile("/mnt/S3/data/dataSet10.txt")

# COMMAND ----------

lineRDD = inputRDD.filter(lambda line:len(line.rstrip())>0)

# COMMAND ----------

rawRDD = lineRDD.map(lambda line:line.split(' ',1))

# COMMAND ----------

punc = [',', '.', ':', ';', '!', '(', ')', '?']
def remove_punc(x):
  for p in punc:
    x= x.lower().replace(p,' ')
  return x.split()

# COMMAND ----------

def clean(x):
  line = ''
  for i in x:
    if i not in punc:
      line += i
  return line

# COMMAND ----------

RDD = rawRDD.map(lambda v:[v[0], remove_punc(v[1])])

# COMMAND ----------

def word_index(x):
  list = []
  for i in x[1]:
    pair = (i,x[0])
    if pair not in list:
      list.append(pair)
  return list

# COMMAND ----------

word_indexRDD = RDD.flatMap(lambda w:(word_index(w))).sortByKey()
word_indexRDD.take(10)

# COMMAND ----------

reduceRDD = word_indexRDD.groupByKey().mapValues(list).sortByKey()

# COMMAND ----------

def count_cal(x):
  count=len(x[1])
  return (count,(x[0],sorted(x[1], key=lambda x:(x[1].split(':')[0]))))

# COMMAND ----------

countRDD = reduceRDD.map(lambda X:count_cal(X))

# COMMAND ----------

finalRDD = countRDD.sortByKey(False)

# COMMAND ----------

outputRDD = finalRDD.map(lambda X: (X[1][0],X[1][1],X[0]))
outputRDD.take(5)

# COMMAND ----------

reduceRDD.coalesce(1).saveAsTextFile("/mnt/S3/output/SparkHW2-1")
outputRDD.coalesce(1).saveAsTextFile("/mnt/S3/output/SparkHW2-2")

# COMMAND ----------


