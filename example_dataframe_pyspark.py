
## Importa el módulo completo pyspark.sql.functions y le asigna un alias (un nombre corto) F
# Claridad y legibilidad: Es muy claro de dónde proviene la función (F.col, F.count). Esto es 
# especialmente útil cuando estás usando funciones con nombres comunes que podrían existir en 
# otros módulos (col, sum, avg, etc.).
# Sí, ante el PEP 8, la declaración import pyspark.sql.functions as F es la forma preferida y 
# más recomendada para importar las funciones de PySpark.

import pyspark.sql.functions as F

# Importa todas las funciones y objetos públicos del módulo pyspark.sql.functions
from pyspark.sql.functions import *
from pyspark.sql.functions import col, count


################################################################################################
                            ####### filter vs Where ###########
# La buena noticia es que filter() y where() son exactamente lo mismo en PySpark.
# recordar que filter y where son con minusculas
df_F_col = df.where(F.col("age") == 25)
df_F_col = df.filter(F.col("age") == 25)


################################################################################################
                        ###########Catalyst#############
# Nativa de Spark SQL: when().otherwise() es una función de PySpark que se traduce directamente a operaciones 
# nativas en la JVM de Spark. Esto significa que Spark puede optimizarla internamente usando su motor Catalyst.
# No hay overhead de serialización: A diferencia de las UDFs de Python, no hay necesidad de serializar y 
# deserializar datos entre la JVM y el proceso de Python, lo que elimina una importante penalización de rendimiento.

Para mapeos pequeños/medianos: Usa when().otherwise(). Es la más eficiente y clara para esos casos.

Para mapeos grandes: Considera un join con un DataFrame que contenga tu tabla de mapeo. Es la solución más escalable.

Para lógica muy compleja o si el rendimiento no es crítico: Usa UDFs de Python como último recurso.


################################################################################################
## filtrar la data tan pronto como sea posible (As Early As Possible - AEAP).
#Spark has a smart optimizer called Catalyst Optimizer. When you apply a filter, 
# the optimizer attempts to "push" that filter as close to the data source as possible.
#For example, if you are reading data from Parquet or ORC, Spark can apply the filter 
# directly when reading the files, avoiding reading irrelevant data from disk. This is an enormous I/O savings.

# No, si realizas un groupBy() en PySpark y no le sigues con una función de agregación 
# (.agg(), .count(), .sum(), etc.), no está completo y no te devolverá un DataFrame con los resultados que esperas.

df = (
    dfatm2.filter(F.col(<Nombre de columna>) == "valor a filtrar") # <-- Filtrar PRIMERO
          .groupBy(<Nombre de columna>)
          .agg(
              F.countDistinct("id").alias("conteo_id"),
              F.count("*").alias("total_count"), # el total de registros por grupo
              F.sum("monto_1").alias("suma_monto_1"),
              F.sum("monto_2").alias("suma_monto_2")
          )
).show()

#Filter con varias columnas 
df.filter(F.col(<nombre_columna>).isin([<valor_1>,<valor_2>,<valor_3>])).orderBy(<nombre_columna>, ascending=True).show()

################################################################################################
# Así es, no puedes ver el contenido de df directamente si solo se usa el groupBy se necesita de un count o funciones de 
# agreciones en un formato de tabla o DataFrame en ese punto.
.groupBy







################################################################################################
# join's

# Left Anti Join
# Un Left Anti Join devuelve todas las filas de la tabla izquierda (df1) que no tienen una coincidencia en la tabla derecha (df2).
# Es útil cuando deseas encontrar registros en df1 que no están presentes en df2
# Ejemplo de Left Anti Join
join_df = df1.join(df2, on=['id','conteo_id'], how='left_anti')

################################################################################################
# Lista de python "bracket" [ recuerda que las listas en Python tienen índices basados en cero
# Lista de Python es una colección ordenada, modificable e indexada de elementos.
# Las listas pueden contener elementos de diferentes tipos, incluyendo números, cadenas, objetos, etc.
['id','conteo_id']
df.select(['columna_A', 'columna_B', 'columna_C'])
df.groupBy(['columna_agrupacion_1', 'columna_agrupacion_2'])
df1.join(df2, on=['clave_comun_1', 'clave_comun_2'], how='inner')
df.orderBy(['columna_orden_1', 'columna_orden_2'], ascending=[True, False])


# Paraiterar las listas de python
# Puedes iterar sobre una lista de Python utilizando un bucle for.
frutas = ["manzana", "banana", "cereza"]
for fruta in frutas:
    print(fruta)

# Adicionar, Eliminar, Modificar Elementos:
# Puedes añadir, eliminar o modificar elementos en una lista de Python utilizando métodos como append(), insert(), remove(), del y asignación directa.
frutas.append("naranja") # Añadir al final
frutas.insert(1, "kiwi") # Insertar en posición específica
frutas.remove("banana") # Eliminar por valor
del frutas[0] # Eliminar por índice
frutas[0] = "uva" # Modificar un elemento    

# Verificar la presencia de un elemento (in):
if "manzana" in frutas:
    print("Hay manzanas")

# Concatenación de listas:
lista1 = [1, 2]
lista2 = [3, 4]
lista_combinada = lista1 + lista2 # [1, 2, 3, 4]

#Por lo tanto, numeros[1:3] selecciona los elementos que comienzan desde el índice 1 hasta (pero sin incluir) el índice 3. 
# Esto te da los elementos 5 (en el índice 1) y 8 (en el índice 2).
# Slicing (Sub-listas):
numeros = [2, 5, 8, 12]
sub_numeros = numeros[1:3]  # [5, 8]

# En Python, el operador ** se utiliza para la exponenciación, es decir, para elevar un número a una determinada potencia.
**
# List Comprehension
# x**2 significa que multiplicara el mismo numero por sí mismo (elevar al cuadrado).
# Por ejemplo, si x es 3, entonces x**2 es 9   3x3 = 9 . si tienes 3 a la 3 3*3*3 = 27
cuadrados = [x**2 for x in range(1, 6)]
cuadrados = [1, 4, 9, 16, 25]

# Conversión de otros tipos a listas:
cadena_a_lista = list("hola") # ['h', 'o', 'l', 'a']
tupla_a_lista = list((1, 2, 3)) # [1, 2, 3]

#max() en Python
# La función max() en Python se utiliza para encontrar el valor máximo en un iterable (como una lista) o entre dos o más argumentos.
# Si se le pasa un iterable, devuelve el elemento más grande de ese iterable.
max_num = max(numeros)

# Definir el esquema de un DataFrame: Cuando creas un DataFrame manualmente.
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([
    StructField("nombre", StringType(), True),
    StructField("edad", IntegerType(), True)
])

################################################################################################

# drop normalmente se usa para eliminar columnas de un DataFrame en PySpark.
df.drop('columna_a', 'columna_b') # Puedes pasar argumentos separados
#Drop dinámico de columnas desempaquetadas
# Si tienes una lista de nombres de columnas que deseas eliminar, puedes usar el 
# operador * para desempaquetar la lista al llamar a drop.
# Esto es útil cuando no conoces de antemano los nombres de las columnas que deseas
columnas_dinamicas_a_eliminar = ['columna_z', 'columna_x'] # Esta lista podría venir de algún cálculo
df_modificado = df.drop(*columnas_dinamicas_a_eliminar)

################################################################################################

# Definir las particiones de un DataFrame en PySpark
# Puedes definir las particiones de un DataFrame utilizando el método repartition().
#Esta técnica se conoce como repartición basada en columnas (o particionado por rango/hash, 
# dependiendo de la implementación interna de Spark para optimización). Es especialmente útil antes de realizar 
# operaciones como groupBy() o join() en esas mismas columnas, ya que asegura que los datos necesarios para esas 
# operaciones ya se encuentran en la misma partición, minimizando futuros shuffles.
# Si solo pones df.repartition(5), Spark hará un "shuffle" (barajado) de los datos para intentar 
# distribuirlos de la forma más uniforme posible en esas 5 particiones, basándose en un hash de las filas.
# Para el número de particiones de "shuffle" (las que se crean después de operaciones como repartition(), 
# groupBy(), join()), el valor por defecto en PySpark (y Apache Spark) es típicamente 200. Esto se controla 
# con la configuración spark.sql.shuffle.partitions.
# Cuando Spark lee datos de sistemas de archivos distribuidos como HDFS, el número de particiones inicial 
# a menudo se basa en el tamaño de los bloques del archivo (comúnmente 128MB por partición).
Así que, para resumir:
No son 200 particiones de 128 MB cada una por defecto.
Spark usa 200 particiones por defecto para los resultados de las operaciones de "shuffle" (spark.sql.shuffle.partitions).
Spark intenta que las particiones iniciales al leer archivos no excedan los 128 MB (basado en spark.sql.files.maxPartitionBytes y el tamaño de bloque de HDFS).
Es fundamental entender la diferencia, ya que la optimización de particiones es uno de los ajustes más importantes para el rendimiento en Spark.
df.repartition(5, 'columna_particion_1', 'columna_particion_2')


################################################################################################
# Cuando le pasas dos argumentos como range(1, 6):
# El primer argumento (1) es el número de inicio (inclusivo). La secuencia comenzará en 1.
# El segundo argumento (6) es el número de fin (exclusivo). La secuencia se detendrá antes de llegar a 6.
range(1, 6)

################################################################################################
#dicionario de python
# No son una estructura de datos intrínseca de PySpark como un DataFrame (que es distribuido), sino 
# que se utilizan en el código Python que interactúa con PySpark.
# Un diccionario de Python es una colección desordenada, modificable e indexada de 
# pares clave-valor. Cada clave debe ser única dentro del diccionario.
# Los diccionarios son útiles para almacenar datos que se pueden acceder mediante una clave específica.
{'id_col': 'id', 'count_col': 'conteo_id'}


################################################################################################
# Las UDFs pueden incurrir en una penalización de rendimiento porque implican serializar y deserializar
# datos entre la JVM (donde corre Spark) y el proceso de Python. Siempre es preferible usar funciones nativas de 
# Spark SQL cuando sea posible, ya que están altamente optimizadas.


# Mapear o transformar datos: Usar diccionarios como tablas de búsqueda dentro de funciones 
# UDF (User Defined Functions) o transformaciones withColumn y map.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# 1. Iniciar una sesión de Spark
spark = SparkSession.builder \
    .appName("MapeoConDiccionarioUDF") \
    .getOrCreate()

# 2. Crear un DataFrame de ejemplo
# Tenemos una columna 'codigo_producto' que queremos mapear a una categoría
data = [
    ("PROD001", 100),
    ("PROD002", 150),
    ("PROD003", 200),
    ("PROD004", 50),
    ("PROD005", 250),
    ("PROD006", 120)
]
columns = ["codigo_producto", "ventas"]
df = spark.createDataFrame(data, columns)

print("--- DataFrame Original ---")
df.show()
# Salida de df.show() para el DataFrame Original:
# +---------------+------+
# |codigo_producto|ventas|
# +---------------+------+
# |        PROD001|   100|
# |        PROD002|   150|
# |        PROD003|   200|
# |        PROD004|    50|
# |        PROD005|   250|
# |        PROD006|   120|
# +---------------+------+

df.printSchema()
# Salida de df.printSchema() para el DataFrame Original:
# root
#  |-- codigo_producto: string (nullable = true)
#  |-- ventas: long (nullable = true)

# 3. Definir el diccionario de mapeo
# Las claves son los códigos de producto y los valores son las categorías
# Puedes tener un valor por defecto para códigos no encontrados
mapeo_categorias = {
    "PROD001": "Electrónica",
    "PROD002": "Electrodomésticos",
    "PROD003": "Muebles",
    "PROD004": "Ropa",
    # Si un código no está en el diccionario, podemos darle una categoría por defecto
    "DEFAULT": "Desconocido"
}

# 4. Crear una función Python para el mapeo
# Esta función tomará un código de producto y devolverá su categoría
def obtener_categoria(codigo):
    # Usamos .get() para poder especificar un valor por defecto si la clave no existe
    return mapeo_categorias.get(codigo, mapeo_categorias["DEFAULT"])

# 5. Registrar la función Python como una UDF (User Defined Function) en Spark
# Le decimos a Spark que la UDF tomará un String y devolverá un String
categoria_udf = udf(obtener_categoria, StringType())

# 6. Usar la UDF con withColumn para agregar la nueva columna
df_con_categorias = df.withColumn(
    "categoria_producto",
    categoria_udf(col("codigo_producto")) # Aplicamos la UDF a la columna 'codigo_producto'
)

print("\n--- DataFrame con Nueva Columna de Categoría ---")
df_con_categorias.show()
# Salida de df_con_categorias.show():
# +---------------+------+-------------------+
# |codigo_producto|ventas|categoria_producto |
# +---------------+------+-------------------+
# |        PROD001|   100|        Electrónica|
# |        PROD002|   150|  Electrodomésticos|
# |        PROD003|   200|            Muebles|
# |        PROD004|    50|                 Ropa|
# |        PROD005|   250|        Desconocido|
# |        PROD006|   120|        Desconocido|
# +---------------+------+-------------------+

df_con_categorias.printSchema()
# Salida de df_con_categorias.printSchema():
# root
#  |-- codigo_producto: string (nullable = true)
#  |-- ventas: long (nullable = true)
#  |-- categoria_producto: string (nullable = true)

# Otro ejemplo: Agregando un código no mapeado para ver el 'DEFAULT'
data_b = [
    ("PROD001", 100),
    ("PROD007", 150), # Este código no está en el diccionario
    ("PROD003", 200)
]
df_b = spark.createDataFrame(data_b, columns)

df_b_con_categorias = df_b.withColumn(
    "categoria_producto",
    categoria_udf(col("codigo_producto"))
)

print("\n--- DataFrame con un código no mapeado (usando DEFAULT) ---")
df_b_con_categorias.show()
# Salida de df_b_con_categorias.show():
# +---------------+------+-------------------+
# |codigo_producto|ventas|categoria_producto |
# +---------------+------+-------------------+
# |        PROD001|   100|        Electrónica|
# |        PROD007|   150|        Desconocido|
# |        PROD003|   200|            Muebles|
# +---------------+------+-------------------+


# 7. Detener la sesión de Spark
spark.stop()

################################################################################################
# Combina lista con diccionarios
# Crear DataFrames a partir de una lista de diccionarios: Cada diccionario representa una fila y las 
# claves del diccionario se convierten en los nombres de las columnas.
# Así que, en esencia, tienes una colección de "registros" (cada uno un diccionario), 
# y esa colección completa está organizada como una lista. Es una estructura de datos muy 
# común y potente en Python para manejar conjuntos de datos estructurados
#Llaves {} para cada diccionario: Dentro de esa lista, cada elemento individual es un diccionario, 
# encerrado en llaves {}. Cada uno de estos diccionarios representa un objeto o entidad,

data = [
    {"nombre": "Alice", "edad": 30, "ciudad": "Nueva York"},
    {"nombre": "Bob", "edad": 24, "ciudad": "Londres"},
    {"nombre": "Charlie", "edad": 35, "ciudad": "París"},
    {"nombre": "David", "edad": 29, "ciudad": "Nueva York"}
]

# Para obtener el primer diccionario de la lista
primer_persona = data[0]
print(f"El primer diccionario es: {primer_persona}")
# Output: El primer diccionario es: {'nombre': 'Alice', 'edad': 30, 'ciudad': 'Nueva York'}

# Para obtener el nombre de la primera persona
nombre_primera_persona = data[0]["nombre"]
print(f"El nombre de la primera persona es: {nombre_primera_persona}")
# Output: El nombre de la primera persona es: Alice

# Para obtener la edad de la segunda persona
edad_segunda_persona = data[1]["edad"]
print(f"La edad de la segunda persona es: {edad_segunda_persona}")
# Output: La edad de la segunda persona es: 24
################################################################################################
# Eso es una tupla en Python.
# Una tupla es una colección ordenada e inmutable de elementos.
(1, 2, 3)

################################################################################################
## collect
