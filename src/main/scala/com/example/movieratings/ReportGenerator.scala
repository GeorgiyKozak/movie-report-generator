package com.example.movieratings

import com.example.movieratings.CsvUtils._
import com.example.movieratings.Domain._
import com.example.movieratings.Errors.InconsistentMovieIdInMovieReviewsFileError
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.csv.CSVRecord

import java.io.{File, FileNotFoundException}
import scala.util.matching.Regex

object ReportGenerator extends StrictLogging {

  val MinReleaseYear = ReleaseYear(1970)
  val MaxReleaseYear = ReleaseYear(1990)
  val MinNumberOfReviews = 1000

  val MovieReviewsFileHeaderKey = 0

  val pattern: Regex = "mv_(\\d+)\\.txt".r

  def main(args: Array[String]): Unit = {
      try {
        // check out CsvUtils for methods to read and write CSV files
        if (args.length < 3) {
          throw new IllegalArgumentException("Not enough arguments")
        }

        args.foreach { arg => if (arg == null) throw new IllegalArgumentException("Cannot pass null values") }

        // Create File instance for the "movie_titles" file
        val pathToFileWithMovies: String = args(0)
        val moviesFile: File = new File(pathToFileWithMovies)

        if (!(moviesFile.exists() && moviesFile.isFile)) {
          throw new FileNotFoundException(s"File with movies doesn't exist: [$pathToFileWithMovies]")
        }

        // Create File instance for the "training_set" directory
        val pathToDirectoryWithMoviesReviewsFiles: String = args(1)
        val movieReviewsDirectory: File = new File(pathToDirectoryWithMoviesReviewsFiles)

        if (!(movieReviewsDirectory.exists() && movieReviewsDirectory.isDirectory)) {
          throw new FileNotFoundException(s"Directory with movies reviews doesn't exist: [$pathToDirectoryWithMoviesReviewsFiles]")
        }

        val pathToReportOutputFile: String = args(2)
        val outputFileWithReport: File = new File(pathToReportOutputFile)

        // Filter all movies by the release year
        val suitableMovies: List[Movie] = readMovies(moviesFile).filter(isSuitableMovie)
        val suitableMoviesMap: Map[MovieID, Movie] = suitableMovies.map { movie => (movie.movieID, movie) }.toMap

        // Retrieve stream of all files in the directory
        val allFilesWithMovieReviews: Stream[File] = movieReviewsDirectory.listFiles().toStream

        // Generate and sort reports by the rating and movie title in descending order
        val sortedMovieReports: List[MovieReport] =
          generateMovieReports(allFilesWithMovieReviews, suitableMoviesMap).sortBy(report => (-report.averageRating, report.movieTitle))

        // Write generated sorted reports to the CSV file
        writeToFile(sortedMovieReports map { convertMovieReportToCsvRecord }, outputFileWithReport)

        logger.info("Report generation completed!")

      } catch {
        case e: Throwable => logger.error(e.getMessage, e)
      }

  }

  def generateMovieReports(filesWithMovieReviews: Stream[File], suitableMovies: Map[MovieID, Movie]): List[MovieReport] = {
    filesWithMovieReviews.filter { isSuitableFileWithMovieReviews(suitableMovies.keySet, _) }.flatMap { fileWithReviews =>
      readMovieReviews(fileWithReviews)
    }.map { case (movieID, movieReviews) =>
      generateMovieReport(suitableMovies: Map[MovieID, Movie], movieID, movieReviews)
    }.toList
  }

  def generateMovieReport(suitableMovies: Map[MovieID, Movie], movieID: MovieID, movieReviews: List[MovieReview]): MovieReport = {
      val movie = suitableMovies.getOrElse(movieID, throw InconsistentMovieIdInMovieReviewsFileError(movieID))
      MovieReport(
        movieTitle = movie.title,
        releaseYear = movie.releaseYear,
        averageRating = calculateAverageRating(movieReviews),
        numberOfReviews = movieReviews.size
      )
  }

  def calculateAverageRating(movieReviews: List[MovieReview]): Double = {
    val sum = movieReviews.map(_.rating.toDouble).sum
    sum / movieReviews.size
  }

  def readMovies(file: File): List[Movie] =
    readFromFileAsList(file) map { record: CSVRecord =>
      convertCsvRecordToMovie(record)
    }

  def readMovieReviews(file: File): Option[(MovieID, List[MovieReview])] = {
    val allRecords: List[CSVRecord] = readFromFileAsList(file)
    val header = allRecords.head
    val movieID: MovieID = MovieID(header.get(MovieReviewsFileHeaderKey).dropRight(1).toShort)

    if ((allRecords.size - 1) > MinNumberOfReviews) {
      val movieReviews: List[MovieReview] = allRecords.tail map { record: CSVRecord =>
        convertCsvRecordToMovieReview(movieID, record)
      }
      Some(movieID, movieReviews)
    } else {
      None
    }
  }

  def isSuitableMovie(movie: Movie): Boolean = !(movie.releaseYear.lt(MinReleaseYear) || MaxReleaseYear.lt(movie.releaseYear))

  def isSuitableFileWithMovieReviews(suitableMovieIds: Set[MovieID], file: File): Boolean = {
    val fileName = file.getName
    fileName match {
      case pattern(id) => suitableMovieIds.contains(MovieID(id.toShort))
      case _ => false
    }
  }

}
