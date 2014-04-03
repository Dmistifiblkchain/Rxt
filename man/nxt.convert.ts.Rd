\name{nxt.convert.ts}
\alias{nxt.convert.ts}
\title{Convert timestamps back and forth between seconds since genesis and POSIXct}
\usage{
nxt.convert.ts(ts, tz = "UTC",
  from.db = length(grep("^(character|POSIX[lc]?t)", class(ts))) == 0,
  genesis.ts = nxt.genesis.ts)
}
\arguments{
  \item{ts}{a vector of timestamp values.  Can be numeric,
  in which case values are assumed to reprsent seconds
  since genesis block; POSIX timestamp objects; or
  character, which is converted to POSIXct}

  \item{tz}{timezone to use when converting character to
  POSIXct.  Defaults to "UTC"}

  \item{from.db}{Boolean. If true, try to convert \code{ts}
  into POSIXct. If false, tries to convert into seconds
  since the genesis block. By default, will try to
  automatically convert numeric into POSIXct and all others
  into numeric (seconds since genesis).}

  \item{genesis.ts}{timestamp of genesis block in POSIXct
  format. Defaults to \code{\link{nxt.genesis.ts}}.}
}
\value{
A vector of timestamps in the desired format.
}
\description{
Timestamps are stored internally in the NXT H2 database in
units of seconds since the genesis block. This function
converts back and forth between the internal format and
POSIXct timestamp objects. If the function is not
explicitly told which direction to convert to it will try
to automatically determine the correct direction based on
inputs.
}
\author{
David M. Kaplan \email{dmkaplan2000@gmail.com}
}

