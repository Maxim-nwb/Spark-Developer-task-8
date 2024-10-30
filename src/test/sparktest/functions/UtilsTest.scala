package ru.sparktest.functions

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import src.homework8.SparkSessionWrapper
import src.homework8.Utils.{BordersCountries, LanguageRating}



class UtilsTest
    extends AnyFlatSpec
    with SparkSessionWrapper
    with Logging
    with BeforeAndAfter
    with DataFrameComparer {

  var path_to_countries = "src/test/resources/countries.json"

  var testDf: DataFrame = _

  before {

    testDf = spark.read.option("multiline", "true")
      .format("json")
      .json(path_to_countries).cache()
      .toDF()
  }

  //1) Технический тест проверка схем
  test_1()

  //2) Технический тест проверка наличия необходимых столбцов
  test_2()

  //1) Функциональный возвращают проверка результата
  test_3()

  private def test_1(): Unit = {
    it should "print BordersCountries schema and show" in {
      val borders_countries_func = BordersCountries(testDf)
      borders_countries_func.printSchema
      borders_countries_func.show(false)
      println("print BordersCountries schema and show  -- PASSED")
    }

    it should "print LanguageRating schema and show" in {
      val language_rating_func = LanguageRating(testDf)
      language_rating_func.printSchema
      language_rating_func.show(false)
      println("print LanguageRating schema and show  -- PASSED")
    }
  }

  private def test_2(): Unit = {
    it should "contain all necessary columns for BordersCountries" in {
      val borders_countries_func = BordersCountries(testDf)

      val necessaryCols = Seq("Country", "NumBorders", "BorderCountries")
      assert(necessaryCols.forall(borders_countries_func.columns.contains))

      println("contain all necessary columns for BordersCountries -- PASSED")
    }

    it should "contain all necessary columns for LanguageRating" in {
      val language_rating_func = LanguageRating(testDf)

      val necessaryCols = Seq("Language", "NumCountries", "Countries")
      assert(necessaryCols.forall(language_rating_func.columns.contains))

      println("contain all necessary columns for LanguageRating -- PASSED")
    }
  }

  private def test_3(): Unit = {
    it should "return correct count for BordersCountries list" in {

      import spark.implicits._

      val resultDf = BordersCountries(testDf)

      val expectedDf = Seq(("CN", List("AFG", "BTN", "MMR", "HKG", "IND", "KAZ", "NPL", "PRK", "KGZ", "LAO", "MAC", "MNG", "PAK", "RUS", "TJK", "VNM"), 16),
        ("RU", List("AZE", "BLR", "CHN", "EST", "FIN", "GEO", "KAZ", "PRK", "LVA", "LTU", "MNG", "NOR", "POL", "UKR"), 14),
        ("BR", List("ARG", "BOL", "COL", "GUF", "GUY", "PRY", "PER", "SUR", "URY", "VEN"), 10)
      ).toDF("Country", "BorderCountries", "NumBorders")

      val intersect = resultDf
        .join(expectedDf,
          resultDf.col("Country") === expectedDf.col("Country"),
          "inner")
        .select(resultDf.col("*"))

      intersect.show(false)
      expectedDf.show(false)

      assertSmallDatasetEquality(expectedDf, intersect, orderedComparison = false)

      println("return correct count for BordersCountries list -- PASSED")
    }

    it should "return correct count for LanguageRating list" in {

      import spark.implicits._

      val resultDf = LanguageRating(testDf).drop("Countries")

      val expectedDf = Seq(("English", 91), ("French", 46), ("Arabic", 25)).
        toDF("Language", "NumCountries")

      val intersect = resultDf
        .join(expectedDf,
          resultDf.col("Language") === expectedDf.col("Language"),
          "inner")
        .select(resultDf.col("*"))

      intersect.show(false)
      expectedDf.show(false)

      assertSmallDatasetEquality(expectedDf, intersect, orderedComparison = false)
      println("return correct count for LanguageRating list -- PASSED")
    }
  }
}
