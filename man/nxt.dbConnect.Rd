\name{nxt.dbConnect}
\alias{nxt.dbConnect}
\title{Make a connection to the NXT H2 database containing the blockchain}
\usage{
nxt.dbConnect(file = "nxt/nxt_db/nxt.h2.db",
  H2.opts = ";DB_CLOSE_ON_EXIT=FALSE;AUTO_SERVER=TRUE",
  H2.prefix = "jdbc:h2:", username = "sa", password = "sa", ...)
}
\arguments{
  \item{file}{Full path to the file containing the NXT H2
  database.}

  \item{H2.opts}{Configuration options for the H2
  connection.  Defaults to
  ";DB_CLOSE_ON_EXIT=FALSE;AUTO_SERVER=TRUE".}

  \item{H2.prefix}{In general, don't touch. Defaults to
  "jdbc:h2:".}

  \item{username}{In general, don't touch. Defaults to
  "sa".}

  \item{password}{In general, don't touch. Defaults to
  "sa".}

  \item{\dots}{Further arguments to the
  \code{\link[RH2]{H2}} driver function (e.g., jars).}
}
\value{
An H2 DB connection object
}
\description{
Recent versions of the NRS NXT client store the blockchain
in a H2 database. This function facilitates connecting to
this database using the RH2 package. This requires RH2 >=
0.2 and NRS client >= 0.8. If you want to connect to the
NXT H2 database while running the NRS client, then one must
instruct the NRS client to open the database with the
AUTO_SERVER=TRUE option using the 'nxt.dbUrl' config
property.
}
\author{
David M. Kaplan \email{dmkaplan2000@gmail.com}
}
\seealso{
\code{\link{nxt.dbDisconnect}}, \code{\link[RH2]{H2}}
}

