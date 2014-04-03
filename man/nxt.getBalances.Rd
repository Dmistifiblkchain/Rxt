\name{nxt.getBalances}
\alias{nxt.getBalances}
\title{Get balances for a set of NXT accounts}
\usage{
nxt.getBalances(con, account.ids = NULL, ts = NULL, id.from.db = TRUE)
}
\arguments{
  \item{con}{Connection object to the NXT H2 database.}

  \item{account.ids}{A vector with NXT IDs of accounts to
  calculate balances for.}

  \item{ts}{Timestamp at which to calculate balances. Can
  be in seconds since genesis or POSIXct format. Defaults
  to calculating balance over all transactions.}

  \item{id.from.db}{Boolean. If \code{TRUE} (default),
  output account IDs in canonical NXT format, otherwise
  leave in signed long format.}
}
\value{
A data.frame with two columns: ACCOUNT_ID and BALANCE
}
\description{
This function queries the NXT blockchain and returns the
NXT balance for a set of account IDs at a specifief time.
}
\details{
If an account ID is not found, it will not appear in the
final data.frame.
}
\author{
David M. Kaplan \email{dmkaplan2000@gmail.com}
}
\seealso{
\code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.id}}
}

