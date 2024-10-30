package src.homework8

import src.homework8.Utils.{BordersCountries, LanguageRating}

object SparkDataFrameAPI extends App
 with Serializable
 with SparkSessionWrapper {

   import spark.implicits._

   var path_to_countries = "src/resources/data/countries.json"


   val countries_ds = spark.read.option("multiline","true")
    .format("json")
     .json(path_to_countries).cache()
     .toDF()

  val t1 = BordersCountries(countries_ds)

  val t2 = LanguageRating(countries_ds)

  spark.stop()
}
