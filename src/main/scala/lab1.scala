import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, udf}
import com.uber.h3core.{H3Core, LengthUnit}
import com.uber.h3core
import com.uber.h3core.util.GeoCoord
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import scala.util.control.Breaks._

object Lab1 {


// A user defined function to calculate H3 indices of given coordinates
  object H3 extends Serializable {
    val instance = H3Core.newInstance()
    val coordinateToH3 = udf { (lat: Double, lon: Double, res: Int) =>
      H3.instance.geoToH3(lat, lon, res)
    }
  }

  // Selecting a resolution for H3 indices
  val res = 10

  // Function to initialize parameters
  def setup: Int = {
    // Suppress logging of Spark
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    var temp = ""
    var sea_level_rise: Int = -1

    // Get sea level raise as user input
    while (sea_level_rise <= -1) {
      try {
        println("Please specify a sea level rise in meters: ")
        temp = scala.io.StdIn.readLine()
        sea_level_rise = temp.toInt
      } catch {
          case e: NumberFormatException =>
            printf(
              temp + " is not an integer. An integer is e.g. 1,2,3,4... \n"
            )
      }
    }
    println("The sea level raise specified is: " + sea_level_rise + "m")
    println("Processing started...")
    sea_level_rise
  }

  def main(args: Array[String]) {

    // Call setup function to initialize program
    val sea_level_rise = setup

    // Create a SparkSession
    val spark =
      SparkSession.builder
        .appName("Lab 1")
        .config("spark.master", "local[*]")
        .getOrCreate

    // Load openStreetMaps dataset as Spark DataFrame
    val df_raw = spark.read.format("orc").load("netherlands.orc")
    val df_OSM = df_raw.select("id", "type", "tags", "lat", "lon")

    // Load ALOS elevation data of coordinates as Spark Dataframe
    var df_ALOS = spark.read.load("parquet/ALPSMLC30_N0*")

    // Join the two data frames on H3 index to get elevation data of places in single df
    val df_OSM_ALOS = joinOnH3Index(df_OSM, df_ALOS)

    // Obtain separate data frames for places that are flooded and places that remain safe
    var df_underWater =
      df_OSM_ALOS.filter(df_OSM_ALOS("elevation") <= sea_level_rise)
    var df_aboveWater =
      df_OSM_ALOS.filter(df_OSM_ALOS("elevation") > sea_level_rise)

    // clean and configure the necessary data frames
    df_underWater = updateDfUnderWater(df_underWater)
    val df_harbour = updateDFHarbour(df_aboveWater)
    df_aboveWater = updateDfAboveWater(df_aboveWater)


    // Obtain df that includes the distances between the places under water and their closest harbours
    val evacuationPlanHarbour =
      findClosestHarbour(df_underWater, df_harbour).cache()
    // Obtain df that includes the distances between the places under water and their closest safe cities
    val evacuationPlanCity =
      findClosestCity(df_underWater, df_aboveWater).cache()

    // Create a data frame with all cities and waterworld with population before evacuations
    val waterworldEntry = spark.createDataFrame(Seq((0L, "Waterworld")))
                               .toDF("Population_aboveWater", "Dest_cities")

    val old_population = evacuationPlanCity.select("Population_aboveWater", "Dest_cities")
                                           .distinct()
                                           .union(waterworldEntry)

    // Compare the two data frames containing distances to nearest harbour and nearest city and obtain the new
    // destinations if a harbour is closer
    var x = isHarbourOrCityCloser(evacuationPlanHarbour, evacuationPlanCity)

    // Split the population of all flooded places by 25 & 75%  in case an evacuation to waterworld is needed
    x = splitPopulation(x)

    // Filter all places which have a harbour closer than a safe city and take 25% of its population as num_evacuees
    // to waterworld
    val waterWorldDestination25 = x
      .filter(x("updated_dest_cities") === "Waterworld")
      .select("Name", "population_25", "updated_dest_cities")
      .withColumnRenamed("Name", "place")
      .withColumnRenamed("population_25", "num_evacuees")
      .withColumnRenamed("updated_dest_cities", "destination")
      .withColumn("num_evacuees", col("num_evacuees").cast(LongType))
    // Filter all places which have a harbour closer than a safe city and take 75% of its population as num_evacuees
    // to their safe city
    val waterWorldDestination75 = x
      .filter(x("updated_dest_cities") === "Waterworld")
      .select("Name", "population_75", "dest_cities")
      .withColumnRenamed("Name", "place")
      .withColumnRenamed("population_75", "num_evacuees")
      .withColumnRenamed("dest_cities", "destination")
      .withColumn("num_evacuees", col("num_evacuees").cast(LongType))

    // If a safe city is closer than a harbor the original destination city will remain. Selecting necessary columns
    val otherCityDestination = x
      .filter(x("updated_dest_cities") =!= "Waterworld")
      .select("Name", "Population_underWater", "updated_dest_cities")
      .withColumnRenamed("Name", "place")
      .withColumnRenamed("Population_underWater", "num_evacuees")
      .withColumnRenamed("updated_dest_cities", "destination")
      .withColumn("num_evacuees", col("num_evacuees").cast(LongType))

    // Join the three data frames with their updated destinations and number of evacuees
    val result = waterWorldDestination25
      .union(otherCityDestination)
      .union(waterWorldDestination75)

    // Arranging a dataframe of destinations with their old and new populations including waterworld
    val y = result
      .drop("place")
      .join(
        old_population,
        result("destination") === old_population("Dest_cities")
      )
      .withColumnRenamed("Population_aboveWater", "old_population")
      .drop("Dest_cities")

    val temp3 = y
      .select("destination", "num_evacuees", "old_population")
      .groupBy("destination")
      .sum("num_evacuees")
      .withColumnRenamed("destination", "destination_1")

    val z = y
      .join(temp3, y("destination") === temp3("destination_1"))
      .withColumn(
        "new_population",
        col("sum(num_evacuees)") + col("old_population")
      )
      .drop("num_evacuees", "destination_1", "sum(num_evacuees)")
      .distinct()

    // Write the resulting data frames as an .orc file
    outputOrc(result)
    outputOrc_2(z)

    // Read back the written orc files and display them on terminal
    val test = spark.read
      .format("orc")
      .load("output_excellent_orc/part-*.snappy.orc")
      .show(30000)
    val test2 = spark.read
      .format("orc")
      .load("output_excellent_population_orc/part-*.snappy.orc")
      .show(300000)



    // Stop the underlying SparkContext
    spark.stop
  }
  def joinOnH3Index(df_OSM: DataFrame, df_ALOS: DataFrame): DataFrame = {
    // Add H3 indices to the osm data frame
    val df_OSM_H3 = df_OSM.withColumn(
      "H3",
      H3.coordinateToH3(col("lat"), col("lon"), lit(res))
    )
    // Add H3 indices to alos data frame. Consider the average elevation of each hexagon
    val df_ALOS_H3 = df_ALOS
      .withColumn(
        "H3",
        H3.coordinateToH3(col("lat"), col("lon"), lit(res))
      )
      .groupBy("H3").avg("elevation")
      .withColumnRenamed("avg(elevation)", "elevation")

    // Join the two data frames
    val result = df_OSM_H3.join(df_ALOS_H3, "H3")
    result
  }

  def splitPopulation(df: DataFrame): DataFrame = {
    // This function adds two new columns to the data frame with populations split with required fractions
    val result = df
      .withColumn("population_25", col("Population_underWater") * 0.25)
      .withColumn("population_75", col("Population_underWater") * 0.75)
    result
  }

  def isHarbourOrCityCloser( df_harbour: DataFrame, df_city: DataFrame): DataFrame = {
    // This function joins the two data frames with distances to closest cities and harbours and sets a flag if the
    // nearest harbour to a place is closer than the nearest city. Then using this flag, it adds a new column with
    // updated destinations which contain waterworld if the nearest harbour is closer than the nearest safe city.
    if(!df_harbour.isEmpty) {
      val df_result = df_city.join(df_harbour, "Name")
        .withColumn("isCityCloser",
          (col("Distance_closest_harbour") > col("Distance_closest_city"))
        )
        .withColumn("updated_dest_cities", when(col("isCityCloser") === false, "Waterworld")
          .otherwise(col("Dest_cities"))
        )
      df_result
    }
    else{
      val df_result = df_city.withColumn("isCityCloser", lit(true))
                             .withColumn("updated_dest_cities", col("Dest_cities"))
      df_result
    }

  }

  def outputOrc(output: DataFrame): Unit = {
    // Function used to output a data frame as .orc file in root folder of the project
    if (
      !new java.io.File(
        System.getProperty("user.dir") + "/output_excellent_orc"
      ).exists
    ) {
      println("Preparing .orc file")
      try output.write.orc(
        System.getProperty("user.dir") + "/output_excellent_orc"
      )
      catch {
        case e: Throwable => println("NOT LINUX compatible")
      } finally {
        println("Output to a compatible Linux path")
        output.write.orc("/output_orc")
      }
    } else {
      println(".orc files already exist")
    }
    // Display number of total evacuees on the terminal
    val num_evacuees_sum = output
      .select(sum("num_evacuees"))
      .first()
      .get(0)

    println("Sum of all evacuees: " + num_evacuees_sum)

  }

  def outputOrc_2(output: DataFrame): Unit = {
    if (
      !new java.io.File(
        System.getProperty("user.dir") + "/output_excellent_population_orc"
      ).exists
    ) {
      println("Preparing .orc file")
      try output.write.orc(
        System.getProperty("user.dir") + "/output_excellent_population_orc"
      )
      catch {
        case e: Throwable => println("NOT LINUX compatible")
      } finally {
        println("Output to a compatible Linux path")
        output.write.orc("/output_2_orc")
      }
    } else {
      println(".orc files already exist")
    }
  }

  def updateDFHarbour(df: DataFrame): DataFrame = {
    // Function that cleans that prepares the data frame that will hold available harbours by:
    // Selecting necessary columns, filtering for harbours only
    val df_updated =
      df.withColumn(
        "Dest_harbour",
        functions.element_at(col("tags"), "name")
      ).withColumn("isHarbour", functions.element_at(col("tags"), "harbour"))
        .drop("id", "type", "tags",  "lat_ALOS", "lon_ALOS", "H3", "elevation") ///////////////////Add elevation to this row
        .filter(col("Dest_harbour").isNotNull)
        .filter(col("isHarbour").isin("yes"))
        .withColumnRenamed("lat", "lat_OSM_Dest")
        .withColumnRenamed("lon", "lon_OSM_Dest")
        .drop("isHarbour")
        .dropDuplicates("Dest_harbour")
    df_updated
  }

  def updateDfAboveWater(df: DataFrame): DataFrame = {
    // Function that cleans the data frame with places that remain above water by:
    // Selecting necessary columns, filtering for necessary places
    val df_updated =
      df.withColumn("Place", functions.element_at(col("tags"), "place"))
        .withColumn("Dest_cities", functions.element_at(col("tags"), "name"))
        .withColumn("Population_aboveWater", functions.element_at(col("tags"), "population"))
        .filter(col("Place").isNotNull)
        .filter(col("Place").isin("city"))
        .withColumnRenamed("lat", "lat_OSM_Dest")
        .withColumnRenamed("lon", "lon_OSM_Dest")
        .drop(
          "id",
          "type",
          "tags",
          "lat_ALOS",
          "lon_ALOS",
          "H3",
        //  "elevation",
          "Place"
        )
        .dropDuplicates("Dest_cities")
    df_updated
  }

  def updateDfUnderWater(df: DataFrame): DataFrame = {
    // Function that cleans the data frame with places that remain below water by:
    // Selecting necessary columns, filtering for necessary places
    val df_updated =
      df.withColumn("Place", functions.element_at(col("tags"), "place"))
        .withColumn("Name", functions.element_at(col("tags"), "name"))
        .withColumn("Population_underWater", functions.element_at(col("tags"), "population"))
        .filter(col("Place").isin("city", "village", "town", "hamlet"))
        .filter(col("Population_underWater").isNotNull)
        .drop("H3")
        .withColumnRenamed("lat", "lat_OSM")
        .withColumnRenamed("lon", "lon_OSM")
        .drop(
          "lat_ALOS",
          "lon_ALOS",
          "id",
          "tags",
          "type",
          "lat_ALOS",
          "lon_ALOS",
          "elevation",
          "Place"
        )
    df_updated
  }

  def findClosestHarbour(df_flooded: DataFrame, df_harbours: DataFrame): DataFrame = {
    // Function that calculates the distances between places under water and the harbours.
    // First a column with the distances is added, than the minimum distance is filtered for each place under water
    var result = df_flooded
      .join(broadcast(df_harbours))
      .withColumn("Distance_closest_harbour",
        sqrt((col("lat_OSM_dest")-col("lat_OSM"))*(col("lat_OSM_dest")-col("lat_OSM"))+
             (col("lon_OSM_dest")-col("lon_OSM"))*(col("lon_OSM_dest")-col("lon_OSM")))
      )
     .drop(
       "id",
       "type",
       "tags",
       "lat_ALOS",
       "lon_ALOS",
       "Distance",
       "lat_OSM",
       "lon_OSM",
       "lat_OSM_Dest",
       "lon_OSM_Dest",
       "Population_underWater"
     )
    // Filtering for only the closest harbours
    val temp = result.groupBy("Name")
                     .min("Distance_closest_harbour")
    // Adding columns lost during groupBy
    result = result.join(temp,
                        result("Distance_closest_harbour") === temp("min(Distance_closest_harbour)"),
              "leftsemi"
     )
   result
 }

 def findClosestCity(df_flooded: DataFrame, df_destinationCities: DataFrame): DataFrame = {
   // Function that calculates the distances between places under water and the safe cities.
   // First a column with the distances is added, than the minimum distance is filtered for each place under water
   var result = df_flooded
     .join(broadcast(df_destinationCities))
     .withColumn("Distance_closest_City",
       sqrt((col("lat_OSM_dest")-col("lat_OSM"))*(col("lat_OSM_dest")-col("lat_OSM"))+
            (col("lon_OSM_dest")-col("lon_OSM"))*(col("lon_OSM_dest")-col("lon_OSM")))
     )
     .select("Name", "Population_underWater", "Population_aboveWater", "Dest_cities", "Distance_closest_City", "elevation") ////////////////////delete elevation
   // Obtain closest city only
   val temp = result.groupBy("Name")
                    .min("Distance_closest_City")
   // Get lost columns
   result = result.join(temp,
                        result("Distance_closest_City") === temp("min(Distance_closest_City)"),
              "leftsemi"
                  )
                  .filter(!(result("Distance_closest_city") === 0))
   result
 }


}