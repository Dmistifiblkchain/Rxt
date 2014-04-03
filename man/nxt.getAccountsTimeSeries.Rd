\name{nxt.getAccountsTimeSeries}
\alias{nxt.getAccountsTimeSeries}
\title{Get a timeseries of transactions for a given set of account IDs}
\usage{
nxt.getAccountsTimeSeries(con, account.ids, start.ts = NULL, end.ts = NULL,
  ts.from.db = TRUE, id.from.db = TRUE, calc.balance = TRUE)
}
\arguments{
  \item{con}{Connection object to the NXT H2 database.}

  \item{account.ids}{A vector with NXT IDs of accounts to
  look for.}

  \item{start.ts}{Minimum timestamp of transactions. Can be
  in seconds since genesis or POSIXct format.}

  \item{end.ts}{Maximum timestamp of transactions. Can be
  in seconds since genesis or POSIXct format.}

  \item{ts.from.db}{Boolean. If \code{TRUE} (default),
  convert timestamps to POSIXct, otherwise keep them in
  seconds since genesis block.}

  \item{id.from.db}{Boolean. If \code{TRUE} (default),
  output block and account IDs in canonical NXT format,
  otherwise leave in signed long format.}

  \item{calc.balance}{Boolean. If \code{TRUE} (default), a
  running balance will be calculated for all transactions.
  This will be the balance for all \code{account.ids} and
  will only reflect transactions occuring between
  \code{start.ts} and \code{end.ts}.}
}
\value{
A data.frame with the following columns:
\item{ACCOUNT_ID}{NXT account ID from list given in
\code{account.ids}}

\item{TIMESTAMP}{Timestamp of transaction}

\item{TRANSACTION_ID}{NXT ID of transaction}

\item{TRANSACTION_DIRECTION}{"SEND", "RECEIVE", or "FORGE"}

\item{TRANSACTION_TYPE}{Integer indicating transaction
type}

\item{TRANSACTION_SUBTYPE}{Integer indicating transaction
subtype}

\item{OTHER_ACCOUNT_ID}{Any other NXT account ID relevant
to transaction}

\item{AMOUNT}{NXT amount of transaction with sign
indicating impact on ACCOUNT_ID (e.g., negative for send
transactions)}

\item{FEE}{NXT fee for transaction with sign indicating
impact on ACCOUNT_ID (e.g., positive for block forgine)}

\item{BALANCE}{Cumsum of \code{AMOUNT+FEE} for all
transaction (only if \code{calc.balance=TRUE})}
}
\description{
This function queries the NXT blockchain and returns an
list of transactions, ordered by timestamp, including
information about the type of transaction, amounts, etc.
for a set of account IDs.
}
\author{
David M. Kaplan \email{dmkaplan2000@gmail.com}
}
\seealso{
\code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}},
\code{\link{nxt.convert.id}}
}

