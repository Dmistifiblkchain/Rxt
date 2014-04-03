\name{nxt.getLastBlock}
\alias{nxt.getLastBlock}
\title{Get last block from the NXT blockchain}
\usage{
nxt.getLastBlock(con, nonzero.fee = FALSE, ...)
}
\arguments{
  \item{con}{Connection object to the NXT H2 database.}

  \item{nonzero.fee}{Boolean. Defaults to \code{FALSE}. If
  \code{TRUE}, get last block that had atleast one
  transaction.}

  \item{\dots}{Additional arguments for
  \code{\link{nxt.getBlocks}}, such as \code{ts.from.db}
  and \code{id.from.db}}
}
\value{
A data.frame with exactly the same information as is found
in the BLOCK table found in the NXT H2 database.
}
\description{
This function queries the NXT blockchain and returns the
most recent block.
}
\author{
David M. Kaplan \email{dmkaplan2000@gmail.com}
}
\seealso{
\code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}},
\code{\link{nxt.convert.id}}, \code{\link{nxt.getBlocks}}
}

