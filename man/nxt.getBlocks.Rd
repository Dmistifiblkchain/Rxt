\name{nxt.getBlocks}
\alias{nxt.getBlocks}
\title{Get a set of blocks from the NXT blockchain}
\usage{
nxt.getBlocks(con, block.ids = NULL, generator.ids = NULL,
  start.ts = NULL, end.ts = NULL, start.height = NULL,
  end.height = NULL, nonzero.fee = FALSE, ts.from.db = TRUE,
  id.from.db = TRUE)
}
\arguments{
  \item{con}{Connection object to the NXT H2 database.}

  \item{block.ids}{A vector of NXT block IDs to query for.}

  \item{generator.ids}{A vector of NXT account IDs to look
  for as the generators of the blocks.}

  \item{start.ts}{Minimum timestamp of block. Can be in
  seconds since genesis or POSIXct format.}

  \item{end.ts}{Maximum timestamp of block. Can be in
  seconds since genesis or POSIXct format.}

  \item{start.height}{Minimum block height.}

  \item{end.height}{Maximum block height.}

  \item{nonzero.fee}{Boolean. Defaults to \code{FALSE}. If
  \code{TRUE}, only return blocks that had transactions in
  them.}

  \item{ts.from.db}{Boolean. If \code{TRUE} (default),
  convert timestamps to POSIXct, otherwise keep them in
  seconds since genesis block.}

  \item{id.from.db}{Boolean. If \code{TRUE} (default),
  output block and account IDs in canonical format,
  otherwise leave in signed long format.}
}
\value{
A data.frame with exactly the same information as is found
in the BLOCK table found in the NXT H2 database.
}
\description{
This function queries the NXT blockchain and returns a set
of blocks meeting a certain set of criteria.
}
\author{
David M. Kaplan \email{dmkaplan2000@gmail.com}
}
\seealso{
\code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}},
\code{\link{nxt.convert.id}}
}

