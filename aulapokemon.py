# Databricks notebook sourceb (ajuste do texto)
spark.conf.set("fs.azure.account.key.databoxhml.blob.core.windows.net","8woRC1uvbZahU9tE/LIzosTmZyfGapMQFBRk8n8sXGLRlm6tUFFq2eHAVOadbAP2YEYRv1a9Nwld+AStdxm2Ww==")
df = spark.read.format('delta').load('wasbs://databox@databoxhml.blob.core.windows.net/pokemons')
df.display()

# COMMAND ----------

import pyspark.sql.functions as fn

# COMMAND ----------

#Seleção
df.select('id', fn.col('altura').alias('ALT')).display()
#filtro
df.filter(fn.col('id')<= 3 ).select('id','nome').display()
#agregação
df.groupBy('altura').agg(fn.sum('peso').alias('Peso'), fn.count('id').alias('Qtd')).display()
#ordenação
df.orderBy( fn.col('peso').asc()).display()

# COMMAND ----------

# DBTITLE 1,Pergunta 1
# quantos pokemons existem no total?
df.distinct().count()


# COMMAND ----------

# DBTITLE 1,Pergunta 2
# quantos kgs pesam todos os pokemons juntos?
df.select( fn.sum('peso')).display()


# COMMAND ----------

# DBTITLE 1,Pergunta 3
# retorne os pokemons que não possuem experiência
df.filter((fn.col('experiencia') > 0 ) | (fn.col('experiencia').isNull())).select('nome').display()


# COMMAND ----------

# DBTITLE 1,Pergunta 4
# retorne o(s) pokemon(s) mais pesado(s)

#df.orderBy( fn.col('peso').desc()).display()
pesomax = df1 = df.agg(fn.max('peso')).first()[0]
df.filter(fn.col('peso') == pesomax).display()



# COMMAND ----------

# DBTITLE 1,Pergunta 5
# retorne o(s) pokemon(s) mais baixo(s)
menor_altura = df1 = df.agg(fn.min('altura')).first()[0]
df.filter(fn.col('altura') == menor_altura).display()

# COMMAND ----------

# DBTITLE 1,Pergunta 6
# quantos pokemons tem mais de 200 pontos de experiencia? E quais são?
exper = df.filter(fn.col('experiencia') > 200 )
exper.count()



# COMMAND ----------

exper = df.filter(fn.col('experiencia') > 200 ).display()

# COMMAND ----------

# DBTITLE 1,Pergunta 7
# quantos pokemons possuem mais de 20m e menos de 1000kg? E quais são?

df7 = df.filter(fn.col('altura') > 20).filter(fn.col('peso') < 1000 )
df7.count()

# COMMAND ----------

df7 = df.filter(fn.col('altura') > 20).filter(fn.col('peso') < 1000 ).display()

# COMMAND ----------

# DBTITLE 1,Pergunta 8
# retorne os pokemons que possuem mais de uma forma

formas = df.select(fn.size(df.formas).alias('contagem'),'*')
formas.filter(fn.col('contagem') > 1).display()


# COMMAND ----------

df_size = df.withColumn('tamanh_lista_forma', fn.size('formas'))
df_size.filter(fn.col('tamanh_lista_forma') >1 ).display()

# COMMAND ----------


