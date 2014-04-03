\name{nxt.getAccountsStats}
\alias{nxt.getAccountsStats}
\title{Get summary account activity statistics for a set of NXT accounts}
\usage{
nxt.getAccountsStats(con, account.ids = NULL, start.ts = NULL,
  end.ts = NULL, ts.from.db = TRUE, id.from.db = TRUE,
  calc.balance = TRUE)
}
\arguments{
  \item{con}{Connection object to the NXT H2 database.}

  \item{account.ids}{A vector with NXT IDs of accounts to
  look for. Defaults to calculating statistics for all
  accounts.}

  \item{start.ts}{Minimum timestamp of transactions and
  blocks. Can be in seconds since genesis or POSIXct
  format. Defaults to start of NXT blockchain.}

  \item{end.ts}{Maximum timestamp of transactions and
  blocks. Can be in seconds since genesis or POSIXct
  format. Defaults to end of NXT blockchain.}

  \item{ts.from.db}{Boolean. If \code{TRUE} (default),
  convert timestamps to POSIXct, otherwise keep them in
  seconds since genesis block.}

  \item{id.from.db}{Boolean. If \code{TRUE} (default),
  output block and account IDs in canonical NXT format,
  otherwise leave in signed long format.}

  \item{calc.balance}{Boolean. If \code{TRUE} (default), a
  balance will be calculated for each account. This balance
  will only reflect transactions occuring between
  \code{start.ts} and \code{end.ts}.}
}
\value{
A data.frame with the following columns corresponding to
statistics for a given set of account IDs over a given time
period: \item{ACCOUNT_ID}{NXT account ID from list given in
\code{account.ids}}

\item{N_REC}{Number of times account received NXT}

\item{N_MESS_REC}{Number of messages received by account}

\item{N_MESS_SENDERS}{Number of distinct accounts sending
messages to this account}

\item{NXT_REC}{Amount of NXT sent to this account}

\item{FIRST_REC}{Timestamp of first transaction received by
account}

\item{LAST_REC}{Timestamp of last NXT transfer to this
account}

\item{N_TRANS}{Total number of transactions of all types
sent by this account}

\item{FEE_PAID}{Total fee paid by this count for sending
transactions}

\item{NXT_SENT}{Amount of NXT sent from this account}

\item{FIRST_SEND}{Timestamp of first time NXT sent from
this account}

\item{LAST_SEND}{Timestamp of last time NXT sent from this
account}

\item{N_SEND}{Number of times NXT sent from this account}

\item{N_MESS_SENT}{Number of messages sent from this
account}

\item{N_MESS_RECIPIENTS}{Number of distinct message
recipients among messages sent from this account}

\item{N_ALIAS_ASSIGNS}{Number of alias assignment
transactions sent by this account. Not necessarily all
alias transactions correspond to different NXT aliases}

\item{N_COLORED_TRANS}{Number of colored coin transactions
sent by this account}

\item{N_FORGED}{Number of blocks forged by this account}

\item{NONZERO_N_FORGED}{Number of blocks forged by this
account with nonzero fee}

\item{FEE_FORGED}{Amount of NXT forged by this account}

\item{FIRST_FORGED}{Timestamp of first block forged by this
account}

\item{LAST_FORGED}{Timestamp of last block forged by this
account}

\item{NONZERO_FIRST_FORGED}{Timestamp of first block forged
by this account with nonzero fee}

\item{NONZERO_LAST_FORGED}{Timestamp of last block forged
by this account with nonzero fee}

\item{FIRST_ACT}{Timestamp of first account activity}

\item{LAST_ACT}{Timestamp of first account activity}

\item{TIME_ACT}{Difference between \code{FIRST_ACT} and
\code{LAST_ACT}}

\item{BALANCE}{Balance of account for time period examined
(only if \code{calc.balance=TRUE})}
}
\description{
This function queries the NXT blockchain and returns
summary statistics regarding transactions and forging for a
set of NXT accounts for a given time period.
}
\author{
David M. Kaplan \email{dmkaplan2000@gmail.com}
}
\seealso{
\code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}},
\code{\link{nxt.convert.id}}
}

