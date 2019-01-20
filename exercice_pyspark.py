from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql import functions as f


sc = SparkContext()
spark = SparkSession(sc)

a = spark.read.csv('ks-projects-201612.csv', header=True, quote='"', escape='"')

quant = a.select('country ').groupBy('country ').agg({'*':'count'})

quant.write.csv('quantidade_pais',mode='overwrite')




category = a.select('category ').groupBy('category ').agg({'*':'count'}).orderBy('count(1)', ascending=False).limit(3)

usd_pled = a.select('category ', a['usd pledged '].cast(FloatType()).alias('usd_pledged')).groupBy('category ').sum('usd_pledged')

joined = category.join(usd_pled, category['category '] == usd_pled['category '], 'inner')

cat = joined.select(category['category '], f.col('sum(usd_pledged)').alias('usd_pledged '))

cat.write.csv('categoria',mode='overwrite')



diff = a.withColumn('usd_difference', a['usd pledged '] - a['goal '])

values = diff.select('country ', 'state ', 'usd_difference').filter(a['country '] == 'US').filter(a['state '] == 'successful')

soma = values.groupBy('state ').sum('usd_difference')

result = soma.select('sum(usd_difference)')

result.write.csv('resultado_diferenca',mode='overwrite')

sc.stop()
