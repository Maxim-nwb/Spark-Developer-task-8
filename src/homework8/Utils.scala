package src.homework8

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

object Utils extends SparkSessionWrapper{
  import spark.implicits._

  def BordersCountries(df:DataFrame ):DataFrame = {
    val rez = df.where(size(col("borders")) >= 5)
      .select(col("cca2").as("Country"), col("borders").as("BorderCountries"), size($"borders").as("NumBorders"))

    rez
  }

  def LanguageRating(df: DataFrame): DataFrame = {
    val short_country = df.select(array_except(array("languages.*"), array(lit(null))).as("languages"), $"cca2").as[ShortCountry]

    val language_by_country = short_country.flatMap(i =>
      i.languages.map(o => CountLanguage(o, 1, Array(i.cca2)))
    )

    val rez = language_by_country.groupByKey(_.Language)
      .mapGroups((_, iter) =>
        iter.reduce((a, b) => {
          a.NumCountries += b.NumCountries
          a.Countries = a.Countries ++ b.Countries
          a
        }
        )
      )

    rez.toDF()
  }
}
