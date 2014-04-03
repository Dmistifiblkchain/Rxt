\name{nxt.newAccountsTimeSeries}
\alias{nxt.newAccountsTimeSeries}
\title{Generate a timeseries of creation of new NXT accounts}
\usage{
nxt.newAccountsTimeSeries(con, timestep = "daily", ts.from.db = TRUE)
}
\arguments{
  \item{con}{Connection object to the NXT H2 database}

  \item{timestep}{Size of timestep to use for generating
  timeseries.  Can be a number of seconds for each timestep
  or "daily" (default), "weekly", "monthly" or "yearly".}

  \item{ts.from.db}{Boolean. If \code{TRUE} (default),
  convert timestamps to POSIXct, otherwise keep them in
  seconds since genesis block.}
}
\value{
A data.frame with the following columns:
\item{TIMESTAMP}{Timestamp at the start of the timestep}

\item{N_ACCOUNT}{Total number of NXT accounts existing at
the end of the timestep}

\item{D_ACCOUNT}{Number of new NXT accounts created during
the timestep}
}
\description{
This function returns a timeseries of the number of new NXT
accounts created on each timestep since the genesis block.
Timestep is controlable, but defaults to daily.
}
\author{
David M. Kaplan \email{dmkaplan2000@gmail.com}
}
\seealso{
\code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}}
}

