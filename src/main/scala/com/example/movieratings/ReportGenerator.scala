package com.example.movieratings

import java.io.{File, FileNotFoundException}
import com.example.movieratings.Domain.{Movie, MovieID, MovieReport, MovieReview}
import scala.collection.mutable
import com.example.movieratings.CsvUtils._
import org.apache.commons.csv.CSVRecord
import scala.util.matching.Regex

object ReportGenerator {

  val MinReleaseYear = 1970
  val MaxReleaseYear = 1990
  val MinNumberOfReviews = 1000

  val pattern: Regex = "mv_(\\d+)\\.txt".r

  private val allSuitableMovies: mutable.HashMap[MovieID, Movie] = mutable.HashMap.empty[MovieID, Movie]

  def main(args: Array[String]): Unit = {
    // check out CsvUtils for methods to read and write CSV files
    if (args.length < 3) {
      throw new IllegalArgumentException("Not enough arguments")
    }
    if (args(0) == "null" || args(1) == "null" || args(2) == "null") {
      throw new IllegalArgumentException("Cannot pass null values")
    }

    // Create File instance for the "movie_titles" file
    val pathToFileWithMovies: String = args(0)
    val moviesFile: File = new File(pathToFileWithMovies)

    if (!(moviesFile.exists() && moviesFile.isFile)) {
      throw new FileNotFoundException("File with movies doesn't exist")
    }

    // Create File instance for the "training_set" directory
    val pathToDirectoryWithMoviesReviewsFiles: String = args(1)
    val movieReviewsDirectory: File = new File(pathToDirectoryWithMoviesReviewsFiles)

    if (!(movieReviewsDirectory.exists() && movieReviewsDirectory.isDirectory)) {
      throw new FileNotFoundException("Directory with movies reviews doesn't exist")
    }

    val pathToReportOutputFile: String = args(2)
    val outputFileWithReport: File = new File(pathToReportOutputFile)

    // Filter all movies by the release year
    val suitableMovies: List[Movie] = getMoviesFromFile(moviesFile).filter(isSuitableMovie)
    // Insert all suitable movies into HashMap
    suitableMovies foreach { movie =>
      allSuitableMovies.put(movie.movieID, movie)
    }

    // Retrieve stream of all files in the directory
    val allFilesWithMovieReviews: Stream[File] = movieReviewsDirectory.listFiles().toStream

    // Generate and sort reports by the rating and movie title in descending order
    val sortedMovieReports: List[MovieReport] = processMoviesReviews(allFilesWithMovieReviews).sortBy(report => (-report.averageRating, report.movieTitle))
    // Write generated sorted reports to the CSV file
    writeToFile(sortedMovieReports map { convertMovieReportToCsvRecord }, outputFileWithReport)

  }

  def processMoviesReviews(filesWithMovieReviews: Stream[File]): List[MovieReport] = {
    filesWithMovieReviews.filter(isSuitableFileWithMovieReviews).flatMap { fileWithReviews =>
      getMovieReviewsFromFile(fileWithReviews)
    }.map { case (movieID, movieReviews) =>
      generateMovieReport(movieID, movieReviews)
    }.toList
  }

  def generateMovieReport(movieID: MovieID, movieReviews: List[MovieReview]): MovieReport = {
      val movie = allSuitableMovies(movieID)
      MovieReport(
        movieTitle = movie.title,
        releaseYear = movie.releaseYear,
        averageRating = calculateAverageRating(movieReviews),
        numberOfReviews = movieReviews.size
      )
  }

  def calculateAverageRating(movieReviews: List[MovieReview]): Double = {
    val sum = movieReviews.map(_.rating).foldLeft(0.0) { (acc, rating) => acc + rating }
    sum / movieReviews.size
  }

  def getMoviesFromFile(file: File): List[Movie] =
    readFromFileAsList(file) map { record: CSVRecord =>
      convertCsvRecordToMovie(record)
    }

  def getMovieReviewsFromFile(file: File): Option[(MovieID, List[MovieReview])] = {
    val allRecords: List[CSVRecord] = readFromFileAsList(file)
    val header = allRecords.head
    val movieID: MovieID = MovieID(header.get(0).split(":").head.toShort)

    if ((allRecords.size - 1) > MinNumberOfReviews) {
      val movieReviews: List[MovieReview] = allRecords.tail map { record: CSVRecord =>
        convertCsvRecordToMovieReview(movieID, record)
      }
      Some(movieID, movieReviews)
    } else {
      None
    }
  }

  def isSuitableMovie(movie: Movie): Boolean = !(movie.releaseYear.value < MinReleaseYear || movie.releaseYear.value > MaxReleaseYear)

  def isSuitableFileWithMovieReviews(file: File): Boolean = {
    val fileName = file.getName
    fileName match {
      case pattern(id) => allSuitableMovies.keySet.contains(MovieID(id.toShort))
      case _ => false
    }
  }

}
