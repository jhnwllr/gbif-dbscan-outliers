
## DBSCAN outlier detection for GBIF occurrence data

Pilherodius pileatus.jpg

![](https://raw.githubusercontent.com/jhnwllr/gbif-dbscan-outliers/master/image_examples/Pilherodius pileatus.jpg)

![](https://raw.githubusercontent.com/jhnwllr/gbif-dbscan-outliers/master/image_examples/Pilherodius%20pileatus.jpg)



This project is intended to detect outliers from GBIF occurrence data using simple haversine distance and clustering (i.e. DBSCAN).  

The project is based roughly on this blog post: https://www.oreilly.com/content/clustering-geolocated-data-using-spark-and-dbscan/

The outlier detection here is based on simple DBSCAN implementation [here][] and then run using spark-submit. 

The end result will be a table of outliers (probably around 300K).

## Build this project

Run this using inside project home directory where the build.sbt is located. 

```
sbt assembly 
```


## Run this project on the cluster 

```



```


## working with and clean up results 

```scala

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._;

// val df = sqlContext.sql("SELECT * FROM prod_h.occurrence").

val df = spark.read.
option("sep", "\t").
option("header", "false").
option("inferSchema", "true").
csv("dbscan_outliers"). 
withColumnRenamed("_c0","specieskey_outlier").
withColumnRenamed("_c1","decimallatitude_outlier").
withColumnRenamed("_c2","decimallongitude_outlier")

val df_occ = sqlContext.sql("SELECT * FROM prod_h.occurrence").
filter($"hasgeospatialissues" === false).
filter($"basisofrecord" =!= "FOSSIL_SPECIMEN").
filter($"basisofrecord" =!= "UNKNOWN").
filter($"basisofrecord" =!= "LIVING_SPECIMEN"). 
filter($"kingdomkey" === 1 || $"kingdomkey" === 6 || $"kingdomkey" === 5). 
filter($"decimallatitude".isNotNull).
filter($"decimallongitude".isNotNull).
select("specieskey","decimallatitude","decimallongitude","gbifid","basisofrecord","datasetkey","kingdom","class","kingdomkey","classkey","eventdate","datasetname")

val df_outliers = df.join(df_occ, 
df_occ("specieskey") === df("specieskey_outlier") &&
df_occ("decimallatitude") === df("decimallatitude_outlier") && 
df_occ("decimallongitude") === df("decimallongitude_outlier"),"left").
drop("decimallatitude_outlier").
drop("decimallongitude_outlier").
drop("specieskey_outlier").
withColumn("date",to_date(from_unixtime($"eventdate" / 1000)))

val df_outliers = df.join(df_occ, 
df_occ("specieskey") === df("specieskey_outlier") &&
df_occ("decimallatitude") === df("decimallatitude_outlier") && 
df_occ("decimallongitude") === df("decimallongitude_outlier"),"left").
drop("decimallatitude_outlier").
drop("decimallongitude_outlier").
drop("specieskey_outlier").
withColumn("date",to_date(from_unixtime($"eventdate" / 1000)))

val df_species_outliers_counts = df.groupBy("specieskey_outlier").
agg(countDistinct("decimallatitude_outlier","decimallongitude_outlier").alias("species_unique_outlier_count")).
withColumnRenamed("specieskey_outlier","specieskey")

val df_dataset_counts = df_occ.groupBy("datasetkey").
agg(count(lit(1)).alias("dataset_occ_count"))

val df_species_counts = df_occ.groupBy("specieskey").
agg(
count(lit(1)).alias("species_occ_count"),
countDistinct("decimallatitude","decimallongitude").alias("species_unique_occ_count"),
countDistinct("datasetkey").alias("species_datasetkey_count")
)

val df_export_1 = df_dataset_counts.join(df_outliers,"datasetkey")
val df_export_2 = df_species_outliers_counts.join(df_export_1,"specieskey")
val df_export = df_species_counts.join(df_export_2,"specieskey").
cache()

df_export.select("specieskey","species_unique_outlier_count","species_unique_occ_count").show()
df_export.filter($"specieskey" === 8503754).select("specieskey","species_unique_outlier_count","species_unique_occ_count").show()

import org.apache.spark.sql.SaveMode
import sys.process._

val save_table_name = "dbscan_outliers_export"

df_export.
write.format("csv").
option("sep", "\t").
option("header", "false").
mode(SaveMode.Overwrite).
save(save_table_name)

// export and copy file to right location 
(s"hdfs dfs -ls")!
(s"rm " + save_table_name)!
(s"hdfs dfs -getmerge /user/jwaller/"+ save_table_name + " " + save_table_name)!
(s"head " + save_table_name)!
// val header = "1i " + "specieskey\tspecies_occ_count\tdatasetkey\tdataset_occ_count\tdecimallatitude\tdecimallongitude\tgbifid\tbasisofrecord\tkingdom\tclass\tkingdomkey\tclasskey\teventdate\tdatasetname\tdate"
val header = "1i " + df_export.columns.toSeq.mkString("""\t""")
Seq("sed","-i",header,save_table_name).!
(s"rm /mnt/auto/misc/download.gbif.org/custom_download/jwaller/" + save_table_name)!
(s"ls -lh /mnt/auto/misc/download.gbif.org/custom_download/jwaller/")!
(s"cp /home/jwaller/" + save_table_name + " /mnt/auto/misc/download.gbif.org/custom_download/jwaller/" + save_table_name)!

```





