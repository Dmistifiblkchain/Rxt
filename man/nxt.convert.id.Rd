\name{nxt.convert.id}
\alias{nxt.convert.id}
\title{Convert NXT IDs between internal H2 DB format and canonical unsigned long
format}
\usage{
nxt.convert.id(id, from.db = any(as.bigz(id) < 0))
}
\arguments{
  \item{id}{a vector of NXT ids.  Can be of any type that
  can be converted to \code{\link[gmp]{bigz}}.}

  \item{from.db}{Boolean. If true, try to convert \code{id}
  to canonical unsigned long NXT ID format. If false,
  converts from unsigned long to signed long. By default,
  will try to automatically convert from the presumed input
  format to the other.}
}
\value{
A vector of character strings containing the IDs in the
desired format.
}
\description{
NXT IDs (e.g., for accounts or transactions or blocks) are
stored internally in the NXT H2 database as signed long.
When querying the database, these are converted to strings
to avoid loss of precision, but IDs are not converted by
the database to the canonical unsigned long format used by
NXT. This function converts IDs back and forth between
unsigned and signed long using \code{\link[gmp]{bigz}}
objects from the gmp package.
}
\details{
For efficiency, results are always returned as character
strings.
}
\author{
David M. Kaplan \email{dmkaplan2000@gmail.com}
}
\seealso{
\code{\link[gmp]{bigz}}
}

