\name{nxt.getTransactions}
\alias{nxt.getTransactions}
\title{Get a set of transactions from the NXT blockchain}
\usage{
nxt.getTransactions(con, block.ids = NULL, sender.ids = NULL,
  recipient.ids = NULL, start.ts = NULL, end.ts = NULL,
  start.height = NULL, end.height = NULL, types = NULL, subtypes = NULL,
  min.amount = NULL, max.amount = NULL, min.fee = NULL, max.fee = NULL,
  ts.from.db = TRUE, id.from.db = TRUE)
}
\arguments{
  \item{con}{Connection object to the NXT H2 database.}

  \item{block.ids}{A vector of NXT block IDs to query for.}

  \item{sender.ids}{A vector with NXT IDs of senders of
  transactions.}

  \item{recipient.ids}{A vector with NXT IDs of recipients
  of transactions.}

  \item{start.ts}{Minimum timestamp of transaction. Can be
  in seconds since genesis or POSIXct format.}

  \item{end.ts}{Maximum timestamp of transaction. Can be in
  seconds since genesis or POSIXct format.}

  \item{start.height}{Minimum height of block corresponding
  to transaction.}

  \item{end.height}{Maximum height of block corresponding
  to transaction.}

  \item{types}{Transaction types to look for (e.g., 0 for
  payment, 1 for messaging, etc.)}

  \item{subtypes}{Transaction subtypes to look for (e.g.,
  types=1, subtypes=1 for alias assignments)}

  \item{min.amount}{Minimum transaction amount (in NXT).}

  \item{max.amount}{Maximum transaction amount (in NXT).}

  \item{min.fee}{Minimum transaction fee (in NXT).}

  \item{max.fee}{Maximum transaction fee (in NXT).}

  \item{ts.from.db}{Boolean. If \code{TRUE} (default),
  convert timestamps to POSIXct, otherwise keep them in
  seconds since genesis block.}

  \item{id.from.db}{Boolean. If \code{TRUE} (default),
  output block and account IDs in canonical NXT format,
  otherwise leave in signed long format.}
}
\value{
A data.frame with exactly the same information as is found
in the TRANSACTION table found in the NXT H2 database.
}
\description{
This function queries the NXT blockchain and returns a set
of transactions meeting a certain set of criteria.
}
\author{
David M. Kaplan \email{dmkaplan2000@gmail.com}
}
\seealso{
\code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}},
\code{\link{nxt.convert.id}}
}

