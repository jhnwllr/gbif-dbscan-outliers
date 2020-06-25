import breeze.linalg.DenseMatrix
import breeze.linalg._
import breeze.numerics._
import nak.cluster._
import nak.cluster.GDBSCAN._

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode


object outliers {

	def main(args: Array[String])
	{

	// command line arguments	
	args.foreach(println)
	val max_count = args(0).toInt // max num of occ that we will process per species	
	val epsilon = args(1).toInt * 1000 // convert to meters. number of kilometers for distance 1500 - 1800 probably good choices	
	val minPoints = args(2).toInt // min number of points per cluster 2-5 is a good value

	// sphere-earth distance
	val haversineDistance: (DenseVector[Double], DenseVector[Double]) => Double = (x, y) =>
	{
		//x and y will have two coordinates, latitude and longitude
		val thisLat = x.valueAt(0)
		val thisLng = x.valueAt(1)
		val otherLat = y.valueAt(0)
		val otherLng = y.valueAt(1)
		//compute the earth distance between locations
		val earthRadius = 3958.7558657441; // earth radius in miles
		val dLat = Math.toRadians(otherLat - thisLat)
		val dLng = Math.toRadians(otherLng - thisLng)
		val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(Math.toRadians(thisLat)) * Math.cos(Math.toRadians(otherLat)) * Math.sin(dLng / 2) * Math.sin(dLng / 2)
		val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
		val dist = earthRadius * c
		val meterConversion = 1609.344
		//convert to meters
		dist * meterConversion
	}
	
	
	// start spark part
	val spark = SparkSession.builder().appName("dbscan_outliers").getOrCreate()
	val sc = spark.sparkContext

	import spark.implicits._
	spark.sparkContext.setLogLevel("ERROR")

	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	
	val SQL_OCCURRENCE = 
	"""
	SELECT DISTINCT					  
	specieskey, 
	kingdomkey,
	decimallatitude, 
	decimallongitude,
	hasgeospatialissues,
	basisofrecord
	FROM  prod_h.occurrence
	WHERE specieskey IS NOT NULL
	"""
	
	// val df_original = sqlContext.sql("SELECT * FROM prod_h.occurrence").
	val df_original = sqlContext.sql(SQL_OCCURRENCE).
		filter($"hasgeospatialissues" === false).
		filter($"basisofrecord" =!= "FOSSIL_SPECIMEN").
		filter($"basisofrecord" =!= "UNKNOWN").
		filter($"basisofrecord" =!= "LIVING_SPECIMEN"). 
		filter($"kingdomkey" === 1 || $"kingdomkey" === 6 || $"kingdomkey" === 5). 
		select(
		"specieskey",
		"decimallatitude",
		"decimallongitude"). 
		distinct().
		na.drop()

	val df_counts = df_original.groupBy("specieskey").count() // counts for filtering 
		
	val df_occ = df_counts.
		join(df_original,"specieskey").
		filter($"count" < max_count && $"count" > 30). // does not do well over 20-50K unique points
		select("specieskey","decimallatitude","decimallongitude")
	  
	val df = df_occ.
		select("specieskey","decimallatitude","decimallongitude").
		withColumn("lat_lon_array",struct("decimallatitude","decimallongitude")).
		groupBy("specieskey").
		agg(collect_list($"lat_lon_array").as("lat_lon_array")).
		select("specieskey","lat_lon_array")
		
	// convert to paired rdd for input into dbscan 	
	val paired_rdd = df.rdd.map(r => {
		val label: Int = r.getAs[Int]("specieskey")
		val content = r.getAs[Seq[Row]]("lat_lon_array")
		val array: Seq[(Double, Double)] = content.map { case Row(decimallatitude: Double, decimallongitude: Double) => (decimallatitude, decimallongitude) }
		(label,array) 
	})
	
	// define dbscan function from nak package 
	// can also be used with Kmeans.euclideanDistance, but use something like 15 degrees
	def dbscan_outliers(point_array : Seq[(Double, Double)],epsilon : Int, minPoints: Int) = {
		val gdbscan = new GDBSCAN(
		DBSCAN.getNeighbours(epsilon = epsilon, distance = haversineDistance),
		DBSCAN.isCorePoint(minPoints = minPoints)
		)
		val v = DenseMatrix(point_array:_*) // convert array to breeze densematrix 
		val clusters = gdbscan cluster v
		val clusterPoints = clusters.map(_.points.map(_.value.toArray))
		val all_cluster_points = clusterPoints.flatten
		all_cluster_points	
	}
	
	val clusters_rdd = paired_rdd.mapValues(dbscan_outliers(_,epsilon,minPoints)) // get clusters

	// extract cluster data as dataframe   
	val df_clusters = clusters_rdd.
		map(r => (r._1,r._2)).toDF().
		withColumnRenamed("_1","specieskey"). 
		withColumn("lat_lon_array",explode($"_2")). // give each own row
		withColumn("decimallatitude",$"lat_lon_array"(0)). // give each own column
		withColumn("decimallongitude",$"lat_lon_array"(1)). // give each own column
		select("specieskey","decimallatitude","decimallongitude").
		cache()
	
	println("---------- cluster count -------------------")
	println(df_clusters.count)

	// join with original data to get outliers 
	val df_join = df_occ. 
		select("specieskey","decimallatitude","decimallongitude").
		withColumnRenamed("decimallatitude","decimallatitude_occ").
		withColumnRenamed("decimallongitude","decimallongitude_occ").
		withColumnRenamed("specieskey","specieskey_occ")

	val df_outliers = df_join.join(df_clusters, 
		df_clusters("specieskey") === df_join("specieskey_occ") &&
		df_clusters("decimallatitude") === df_join("decimallatitude_occ") && 
		df_clusters("decimallongitude") === df_join("decimallongitude_occ"),"left").
		filter($"specieskey".isNull). // those without any cluster will be outliers
		select(specieskey_occ,decimallatitude_occ,decimallongitude_occ). 
		cache()
		
	println("---------- outlier count -------------------")
	println(df_outliers.count())
	df_outliers.show(100,false)
	
	// Output results	
	val save_table_name = "dbscan_outliers"
	
	df_outliers.
		select("specieskey_occ","decimallatitude_occ","decimallongitude_occ").
		write.format("csv").
		option("sep", "\t").
		option("header", "false").
		mode(SaveMode.Overwrite).
		save(save_table_name)
	
  }

}

