package com.example.movieratings

import Domain.MovieID

object Errors {

  case class InconsistentMovieIdInMovieReviewsFileError(movieID: MovieID) extends Throwable {
    val message = s"Inconsistent [$movieID] MovieID in MovieReviews file"
    override def getMessage: String = message
  }


}
