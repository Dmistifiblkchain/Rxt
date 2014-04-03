\name{nxt.getAliases}
\alias{nxt.getAliases}
\title{EXPERIMENTAL - Get a set of alias assignments from the NXT blockchain}
\usage{
nxt.getAliases(con, alias.names = NULL, alias.uri = NULL,
  owner.ids = NULL, ts = NULL, ts.from.db = TRUE, id.from.db = TRUE)
}
\arguments{
  \item{con}{Connection object to the NXT H2 database.}

  \item{alias.names}{Either a vector of alias names or a
  single regular expression character string of the sort
  recognized by an H2 database. To search for a single
  alias name use
  \code{alias.names=c("ALIASNAME","ALIASNAME")}.}

  \item{alias.uri}{A single regular expression character
  string of the sort recognized by an H2 database. Will
  match only valid assignment, not alias assignments before
  \code{ts}.}

  \item{owner.ids}{A vector with NXT IDs of alias owners.}

  \item{ts}{Timestamp at which to look for alias. Can be in
  seconds since genesis or POSIXct format. Defaults to end
  of blockchain.}

  \item{ts.from.db}{Boolean. If \code{TRUE} (default),
  convert timestamps to POSIXct, otherwise keep them in
  seconds since genesis block.}

  \item{id.from.db}{Boolean. If \code{TRUE} (default),
  output transaction and account IDs in canonical NXT
  format, otherwise leave in signed long format.}
}
\value{
A data.frame containing aliases and info regarding when
they were assigned.
}
\description{
This function queries the NXT blockchain and returns a set
of alias assignments meeting a certain set of criteria.
Requires attachment info be stored in a special schema in
the H2 database, which is not yet in the standard NXT
release.  See
\href{https://github.com/dmkaplan2000/nxt_dmk/blob/feature/dmk/src/java/nxt/AttachmentSchema.java}{here}.
}
\details{
Returned alias URIs will reflect valid alias assignments at
given timestamp.
}
\author{
David M. Kaplan \email{dmkaplan2000@gmail.com}
}
\seealso{
\code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}},
\code{\link{nxt.convert.id}}
}

