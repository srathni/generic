import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import array_contains
#Create spark session
spark = SparkSession\
.builder\
.appName("tst")\
.master("local[4]")\
.config("spark.driver.memory","1g")\
.getOrCreate()
print("""\n\n\n""")
derivation_lookup = [("mTrade",1,"event1","account_num","56","no"), ("mTrade",2,"event1","seller_name","ford","no"), \
("mTrade",3,"event2","subaccount","4566","no"), \
      ("mTrade",4,"event2","status","active","no"),("mTrade",5,"event2","seller_name","ford,gm,bmw,suzuki","yes"),\
      ("mTrade",6,"event1","subaccount","4545,56,77","yes")]

columns= ["sor","activity_line_sid","activity_line_name","selection_key","selection_value","is_list"]
df = spark.createDataFrame(data = derivation_lookup, schema = columns)#actual code will get data for this df from derivation_lookup table

print("lookup table")
df.show()

pivotDF = df.groupBy("activity_line_name","sor").pivot("selection_key").agg(first("selection_value"))
print("pivoted lookup table")
pivotDF.show()
pivotDF.createOrReplaceTempView("derivation_lookup")

fci=[("mTrade","76","jeep","inactive",399),("mTrade","56","ford","inactive",4545),\
("mTrade","44","bmw","active",4566), ("mTrade","99","honda","active",9878)]

columns2=["sor","account_num","seller_name","status","subaccount"]

fci_df = spark.createDataFrame(data = fci, schema = columns2)
print("fci data")
fci_df.show()
fci_df.createOrReplaceTempView("fci")

events_df = spark.sql("""
    SELECT dl.activity_line_name,fci.*
    FROM fci
    INNER JOIN derivation_lookup dl
    ON
    (
        (
            dl.activity_line_name = "event1" and
            fci.seller_name = dl.seller_name and
            fci.account_num = dl.account_num and
            array_contains(split(dl.subaccount, ","), CAST(fci.subaccount AS STRING))
        )
        OR
        (
            dl.activity_line_name = "event2" AND
            fci.subaccount = dl.subaccount AND
            fci.status = dl.status AND
            array_contains(split(dl.seller_name, ","), fci.seller_name)
        )
    )
""")

print("event data")
events_df.show()
