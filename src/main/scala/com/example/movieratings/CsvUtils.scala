package com.example.movieratings

import org.apache.commons.csv.{CSVFormat, CSVRecord}

import java.io.{File, FileReader, FileWriter}
import scala.collection.JavaConverters._
import com.example.movieratings.Domain.{CustomerID, Movie, MovieID, MovieReport, MovieReview, ReleaseYear}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/** Helper methods to work with CSV files.
 *
 * more info: https://commons.apache.org/proper/commons-csv
 */
object CsvUtils {

  val MovieIDKey = 0
  val MovieReleaseYearKey = 1
  val MovieTitleKey = 2
  val CustomerIDKey = 0
  val RatingKey = 1
  val ReviewDateKey = 2

  /** Reads records from a CSV file.
   *
   * Examples:
   * val record: CSVRecord = ...
   * record.size() // the number of columns in this record
   * record.get(2) // returns the third column as a String
   */
  def readFromFileAsList(file: File): List[CSVRecord] = {
    val reader = new FileReader(file)
    try {
      CSVFormat.DEFAULT.parse(reader).asScala.toList
    } finally {
      reader.close()
    }
  }

  /** Writes records to a file in a CSV format.
   *
   * Each record is a List[Any], e.g. this snippet writes two records to a file:
   * val records = List(
   *   List("John", 41, "Plumber"),
   *   List("Misato-san", 29, "Operations Director")
   * )
   * CsvUtils.writeToFile(records, new File("employees.csv"))
   */
  def writeToFile(records: Iterable[List[Any]], file: File): Unit = {
    val writer = new FileWriter(file)
    try {
      val printer = CSVFormat.DEFAULT.print(writer)
      records.foreach(r => printer.printRecord(r.asJava))
      printer.close(true)
    } finally {
      writer.close()
    }
  }

  def convertCsvRecordToMovie(record: CSVRecord): Movie = {
    Movie(
      movieID = MovieID(record.get(MovieIDKey).toShort),
      releaseYear = if (record.get(MovieReleaseYearKey) == "NULL") { ReleaseYear(0) } else { ReleaseYear(record.get(MovieReleaseYearKey).toShort) },
      title = record.get(MovieTitleKey)
    )
  }

  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def convertCsvRecordToMovieReview(movieID: MovieID, record: CSVRecord): MovieReview = {
    val reviewDate = LocalDate.parse(record.get(ReviewDateKey), dateFormatter)
    MovieReview(
      movieID = movieID,
      customerID = CustomerID(record.get(CustomerIDKey).toLong),
      rating = record.get(RatingKey).toByte,
      date = reviewDate
    )
  }

  def convertMovieReportToCsvRecord(movieReport: MovieReport): List[Any] =
    List(movieReport.movieTitle, movieReport.releaseYear.value, movieReport.averageRating, movieReport.numberOfReviews)

}
