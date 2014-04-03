\name{nxt.dbDisconnect}
\alias{nxt.dbDisconnect}
\title{Disconnect from NXT H2 database}
\usage{
nxt.dbDisconnect(con)
}
\arguments{
  \item{con}{H2 connection object}
}
\value{
Boolean indicating success of operation
}
\description{
Currently just a wrapper for
\code{[\link[DBI]{dbDisconnect}}.
}
\author{
David M. Kaplan \email{dmkaplan2000@gmail.com}
}
\seealso{
\code{\link{nxt.dbConnect}}
}

