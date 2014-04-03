\name{nxt.getActivityTimeSeries}
\alias{nxt.getActivityTimeSeries}
\title{Generate a timeseries of NXT transaction activity}
\usage{
nxt.getActivityTimeSeries(con, timestep = "daily", ts.from.db = TRUE)
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

\item{N_TRANS}{Total number of transactions of all types
per timestep}

\item{NXT_SENT}{Amount of NXT sent per timestep}

\item{FEE_PAID}{Total fees paid per timestep}

\item{N_SEND}{Number of times NXT sent per timestep}

\item{N_MESSAGE}{Number of messages sent per timestep}

\item{N_ALIAS_ASSIGN}{Number of alias assignment
transactions per timestemp}

\item{N_COLORED_TRANS}{Number of colored coin transactions
per timestep}

\item{N_DIST_SENDER}{Number of distinct transaction sender
accounts}

\item{N_DIST_RECIP}{Number of distinct transaction
recipient accounts}

\item{N_BLOCK}{Number of blocks per timestep}

\item{N_BLOCK_FEE}{Number of blocks with fee per timestep}

\item{N_BLOCK_NOFEE}{Number of blocks without fee per
timestep}

\item{N_DIST_GENER}{Number of distinct block generators per
timestep}

\item{N_DIST_GENER_FEE}{Number of distinct block generators
with fee per timestep}

\item{MEAN_BASE_TARGET}{Mean of base target}

\item{MEAN_INV_TARGET}{Mean of 1.0/(base target)}

\item{MIN_BASE_TARGET}{Minimum base target per timestep}

\item{MAX_BASE_TARGET}{Maximum base target per timestep}
}
\description{
This function returns a timeseries of the number of NXT
transactions by type, amount of NXT transferred, fees paid,
etc. on each timestep since the genesis block.  Timestep is
controlable, but defaults to daily.
}
\author{
David M. Kaplan \email{dmkaplan2000@gmail.com}
}
\seealso{
\code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}}
}

