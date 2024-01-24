package com.example.movieratings

import java.time.LocalDate

object Domain {

  case class MovieID(id: Short) extends AnyVal

  case class ReleaseYear(value: Short) extends AnyVal

  case class CustomerID(id: Long) extends AnyVal

  case class Movie(movieID: MovieID, releaseYear: ReleaseYear, title: String)

  case class MovieReview(movieID: MovieID, customerID: CustomerID, rating: Byte, date: LocalDate)

  case class MovieReport(movieTitle: String, releaseYear: ReleaseYear, averageRating: Double, numberOfReviews: Long)

}
