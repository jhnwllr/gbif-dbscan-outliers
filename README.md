
## DBSCAN outlier detection for GBIF occurrence data

This project detects outliers in GBIF occurrence data using clustering (i.e. DBSCAN) with haversine distance (sphere-earth distance).  

![](https://raw.githubusercontent.com/jhnwllr/gbif-dbscan-outliers/master/image_examples/Pilherodius%20pileatus.jpg)

It was inspired by this blog post: https://www.oreilly.com/content/clustering-geolocated-data-using-spark-and-dbscan/

The end result will be a table of outliers (probably around 1M-300K depending on the settings). Running on all Animal, Plant, and Fungi species with <20K unique points takes around 1 hour. 

> An **outlier** of course does not mean that the point is in any way bad or a mistake. It simply is a point that is not close to any other points, which might mean the user should probably look at these points more closely. Outliers can be mistakes but they can also be valuable observations from an undersampled region. There is no objective way from distance alone to tell the difference. 

## Build this project

Run this using inside project home directory where the `build.sbt` is located. 

```
sbt assembly 
```

## Run this project on the cluster 

The assembly jar `gbif-dbscan-outliers-assembly-0.1.jar` should be run with 3 cmd-line arguments: 

1. **max_count** : the max number of unique lat lon points per species to run. Should be between 20K - 50K. Species with more occurrences are not run. 
2. **epsilon** : the distance parameter in km for the dbscan algorithm. 1500 - 1800 probably good values
3. **minPoints** : the minimum number of points for a cluster. 3 - 5 probably good values

```
spark2-submit --num-executors 40 --executor-cores 5 --driver-memory 8g --driver-cores 4 --executor-memory 16g gbif-dbscan-outliers-assembly-0.1.jar 20000 1500 3
```

This will save a file in hdfs called `dbscan_outliers`. 

## Plotted examples 

![](https://raw.githubusercontent.com/jhnwllr/gbif-dbscan-outliers/master/image_examples/Amoya%20gracilis.jpg)

![](https://raw.githubusercontent.com/jhnwllr/gbif-dbscan-outliers/master/image_examples/Bryum%20chryseum.jpg)

![](https://raw.githubusercontent.com/jhnwllr/gbif-dbscan-outliers/master/image_examples/Chalceus%20erythrurus.jpg)


![](https://raw.githubusercontent.com/jhnwllr/gbif-dbscan-outliers/master/image_examples/Eucalyptus%20bakeri.jpg)



## working with and clean up results 

The raw output, while pretty good, probably needs some extra filtering before usable. 

For example, the species below has way too many "outliers" flagged. One fix for this would be to white-list species that have more than 5% of its unique points as outliers. 

![](https://raw.githubusercontent.com/jhnwllr/gbif-dbscan-outliers/master/image_examples/Ditropichthys%20storeri.jpg)


Other species might have many points sitting exactly on one location from many observations over time. One could take the unique eventdate count of each occurrence and again white-list those occurrence locations for that species. Probably one would **NOT** want to white-list points which are simply duplicates on the same day.

Below is one way to process the results into something more usuable for filtering. 
 
```scala

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._;

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





