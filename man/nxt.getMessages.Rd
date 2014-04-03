\name{nxt.getMessages}
\alias{nxt.getMessages}
\title{EXPERIMENTAL - Get a set of messages from the NXT blockchain}
\usage{
nxt.getMessages(con, sender.ids = NULL, recipient.ids = NULL,
  start.ts = NULL, end.ts = NULL, start.height = NULL,
  end.height = NULL, message.to.char = TRUE, ts.from.db = TRUE,
  id.from.db = TRUE)
}
\arguments{
  \item{con}{Connection object to the NXT H2 database.}

  \item{sender.ids}{A vector with NXT IDs of senders of the
  messages.}

  \item{recipient.ids}{A vector with NXT IDs of recipients
  of messages.}

  \item{start.ts}{Minimum timestamp of transaction. Can be
  in seconds since genesis or POSIXct format.}

  \item{end.ts}{Maximum timestamp of transaction. Can be in
  seconds since genesis or POSIXct format.}

  \item{start.height}{Minimum height of block corresponding
  to transaction.}

  \item{end.height}{Maximum height of block corresponding
  to transaction.}

  \item{message.to.char}{Boolean. If \code{TRUE} (default),
  will attempt to convert message from a hex string to a
  normal character string.}

  \item{ts.from.db}{Boolean. If \code{TRUE} (default),
  convert timestamps to POSIXct, otherwise keep them in
  seconds since genesis block.}

  \item{id.from.db}{Boolean. If \code{TRUE} (default),
  output block and account IDs in canonical NXT format,
  otherwise leave in signed long format.}
}
\value{
A data.frame containing transaction id, sended id,
recipient id, blockchain height, transaction timestamp,
message as a hex string and possibly message as a character
vector (if \code{message.to.char=TRUE}).
}
\description{
This function queries the NXT blockchain and returns a set
of messages meeting a certain set of criteria. Requires
attachment info be stored in a special schema in the H2
database, which is not yet in the standard NXT release.
See
\href{https://github.com/dmkaplan2000/nxt_dmk/blob/feature/dmk/src/java/nxt/AttachmentSchema.java}{here}.
}
\author{
David M. Kaplan \email{dmkaplan2000@gmail.com}
}
\seealso{
\code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}},
\code{\link{nxt.convert.id}}, \code{\link{HexStringToRaw}}
}

