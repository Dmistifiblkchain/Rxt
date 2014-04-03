#' Converts hex strings to raw R objects
#' 
#' Converts hex strings (e.g., "ac6fef") to a \code{\link{raw}} R object
#' 
#' @param s Single hex string. Use \code{\link{apply}} or \code{\link{sapply}} 
#'   to work with vectors of hex strings
#'   
#' @return Raw R object corresponding to hex string
#'   
#' @seealso \code{\link{raw}}, \code{\link{rawToChar}},
#'   \code{\link{RawToHexString}}
#'   
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @export
HexStringToRaw <- function(s) {
  n = nchar(s)/2
  x = raw(length=n)
  for (k in 1:n) {
    x[k] = as.raw(strtoi(substr(s,2*k-1,2*k),16L))
  }
  return(x)
}

#' Converts raw R objects to hex string
#' 
#' Converts a \code{\link{raw}} R object to a hex strings (e.g., "ac6fef")
#' 
#' @param x Object of type \code{\link{raw}}
#'   
#' @return Hex string corresponding to raw object
#'   
#' @seealso \code{\link{raw}}, \code{\link{rawToChar}},
#'   \code{\link{HexStringToRaw}}
#'   
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @export
RawToHexString <- function(x) { paste(as.character(x),collapse="") }

#' EXPERIMENTAL - Get a set of messages from the NXT blockchain
#' 
#' This function queries the NXT blockchain and returns a set of messages 
#' meeting a certain set of criteria. Requires attachment info be stored in a 
#' special schema in the H2 database, which is not yet in the standard NXT 
#' release.  See 
#' \href{https://github.com/dmkaplan2000/nxt_dmk/blob/feature/dmk/src/java/nxt/AttachmentSchema.java}{here}.
#' 
#' @param con Connection object to the NXT H2 database.
#' @param sender.ids A vector with NXT IDs of senders of the messages.
#' @param recipient.ids A vector with NXT IDs of recipients of messages.
#' @param start.ts Minimum timestamp of transaction. Can be in seconds since 
#'   genesis or POSIXct format.
#' @param end.ts Maximum timestamp of transaction. Can be in seconds since 
#'   genesis or POSIXct format.
#' @param start.height Minimum height of block corresponding to transaction.
#' @param end.height Maximum height of block corresponding to transaction.
#' @param message.to.char Boolean. If \code{TRUE} (default), will attempt to 
#'   convert message from a hex string to a normal character string.
#' @param ts.from.db Boolean. If \code{TRUE} (default), convert timestamps to 
#'   POSIXct, otherwise keep them in seconds since genesis block.
#' @param id.from.db Boolean. If \code{TRUE} (default), output block and account
#'   IDs in canonical NXT format, otherwise leave in signed long format.
#'   
#' @return A data.frame containing transaction id, sended id, recipient id,
#'   blockchain height, transaction timestamp, message as a hex string and
#'   possibly message as a character vector (if \code{message.to.char=TRUE}).
#'   
#' @seealso \code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}}, 
#'   \code{\link{nxt.convert.id}}, \code{\link{HexStringToRaw}}
#'   
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @export
nxt.getMessages <- function(con,sender.ids=NULL,recipient.ids=NULL,
                            start.ts=NULL,end.ts=NULL,
                            start.height=NULL,end.height=NULL,
                            message.to.char=TRUE,
                            ts.from.db=TRUE,id.from.db=TRUE) {
  w="WHERE TRUE"
  
  if (!is.null(sender.ids)) {
    sender.ids=paste(nxt.convert.id(sender.ids,from.db=FALSE),collapse=",")
    w=paste(w," AND SENDER_ID IN (",sender.ids,")",sep="")
  }
  
  if (!is.null(recipient.ids)) {
    recipient.ids=paste(nxt.convert.id(recipient.ids,from.db=FALSE),collapse=",")
    w=paste(w," AND RECIPIENT_ID IN (",recipient.ids,")",sep="")
  }
  
  if (!is.null(start.ts)) {
    start.ts=nxt.convert.ts(start.ts,from.db=FALSE)
    w=paste(w," AND TIMESTAMP>=",start.ts,sep="")
  }
  
  if (!is.null(end.ts)) {
    end.ts=nxt.convert.ts(end.ts,from.db=FALSE)
    w=paste(w," AND TIMESTAMP<=",end.ts,sep="")
  }
  
  if (!is.null(start.height)) {
    w=paste(w," AND HEIGHT>=",start.height,sep="")
  }
  
  if (!is.null(end.height)) {
    w=paste(w," AND HEIGHT<=",end.height,sep="")
  }
  
  b=dbGetQuery(con,paste("SELECT CAST(ID AS VARCHAR) AS TRANSACTION_ID,
                         CAST(SENDER_ID AS VARCHAR) AS SENDER_ID,
                         CAST(RECIPIENT_ID AS VARCHAR) AS RECIPIENT_ID,
                         HEIGHT, 
                         TIMESTAMP, 
                         MESSAGE
                         FROM PUBLIC.TRANSACTION t JOIN 
                         ATTACHMENT.MESSAGING_ARBITRARY_MESSAGE m
                         ON t.ID=m.TRANSACTION_ID",w))
  
  bigint.cols = c("TRANSACTION_ID","RECIPIENT_ID","SENDER_ID")
  for(i in bigint.cols) b[,i] = nxt.convert.id(b[,i],from.db=id.from.db)
  
  b$TIMESTAMP = nxt.convert.ts(b$TIMESTAMP,from.db=ts.from.db)
  
  row.names(b)=as.character(b$TRANSACTION_ID)
  
  if (message.to.char)
    b$MESSAGE_CHAR = sapply(b$MESSAGE,
                            function(x) {xx=HexStringToRaw(x); 
                                         if (!any(xx==0)) return(rawToChar(xx))
                                         else return(NA_character_)} )
  
  return(b)
}

#' EXPERIMENTAL - Get a set of alias assignments from the NXT blockchain
#' 
#' This function queries the NXT blockchain and returns a set of alias 
#' assignments meeting a certain set of criteria. Requires attachment info be 
#' stored in a special schema in the H2 database, which is not yet in the 
#' standard NXT release.  See 
#' \href{https://github.com/dmkaplan2000/nxt_dmk/blob/feature/dmk/src/java/nxt/AttachmentSchema.java}{here}.
#' 
#' Returned alias URIs will reflect valid alias assignments at given timestamp.
#' 
#' @param con Connection object to the NXT H2 database.
#' @param alias.names Either a vector of alias names or a single regular 
#'   expression character string of the sort recognized by an H2 database. To 
#'   search for a single alias name use 
#'   \code{alias.names=c("ALIASNAME","ALIASNAME")}. Note: alias names and regular
#'   expressions will be made lower case before use.
#' @param alias.uri A single regular expression character string of the sort 
#'   recognized by an H2 database. Will match only valid assignment, not alias 
#'   assignments before \code{ts}.
#' @param owner.ids A vector with NXT IDs of alias owners.
#' @param ts Timestamp at which to look for alias. Can be in seconds since 
#'   genesis or POSIXct format. Defaults to end of blockchain.
#' @param ts.from.db Boolean. If \code{TRUE} (default), convert timestamps to 
#'   POSIXct, otherwise keep them in seconds since genesis block.
#' @param id.from.db Boolean. If \code{TRUE} (default), output transaction and 
#'   account IDs in canonical NXT format, otherwise leave in signed long format.
#'   
#' @return A data.frame containing aliases and info regarding when they were 
#'   assigned.
#'   
#' @seealso \code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}}, 
#'   \code{\link{nxt.convert.id}}
#'   
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @export
nxt.getAliases <- function(con,alias.names=NULL,alias.uri=NULL,
                           owner.ids=NULL,ts=NULL,
                           ts.from.db=TRUE,id.from.db=TRUE) {
  w="WHERE TRUE"
  
  if (!is.null(owner.ids)) {
    owner.ids=paste(nxt.convert.id(owner.ids,from.db=FALSE),collapse=",")
    w=paste(w," AND SENDER_ID IN (",owner.ids,")",sep="")
  }
  
  if (!is.null(alias.names)) {
    alias.names=tolower(alias.names)
    if (length(alias.names)==1) {
      w=paste(w," AND LOWER(a.NAME) REGEXP '",alias.names,"'",sep="")
    } else {
      alias.names=paste(nxt.convert.id(alias.names,from.db=FALSE),collapse="'','")
      w=paste(w," AND LOWER(a.NAME) IN ('",alias.names,"'')",sep="")
    }
  }
  
  if (!is.null(ts)) {
    ts=nxt.convert.ts(ts,from.db=FALSE)
    w=paste(w," AND TIMESTAMP<=",ts,sep="")
  }
  
  h = ""
  if (!is.null(alias.uri)) {
    h=paste("WHERE a.URI REGEXP '",alias.uri,"'",sep="")
  }
  
  dbSendUpdate(con,"DROP TABLE IF EXISTS RXTALIASTMP")
  q=paste("CREATE LOCAL TEMPORARY TABLE RXTALIASTMP AS
          SELECT t.SENDER_ID AS OWNER_ID, a.NAME,
          max(t.BLOCK_TIMESTAMP) AS BLOCK_TIMESTAMP,
          count(*) AS NUM_REASSIGNS,
          min(t.BLOCK_TIMESTAMP) AS FIRST_ASSIGN
          FROM PUBLIC.TRANSACTION t JOIN 
          ATTACHMENT.MESSAGING_ALIAS_ASSIGNMENT a
          ON t.ID=a.TRANSACTION_ID",w,
          "GROUP BY t.SENDER_ID, a.NAME")
  dbSendUpdate(con,q)
  
  qq=paste("SELECT a.TRANSACTION_ID, r.OWNER_ID, r.NAME, a.URI,
           r.BLOCK_TIMESTAMP, r.NUM_REASSIGNS, r.FIRST_ASSIGN
           FROM PUBLIC.TRANSACTION t JOIN 
           ATTACHMENT.MESSAGING_ALIAS_ASSIGNMENT a
           ON t.ID=a.TRANSACTION_ID JOIN
           RXTALIASTMP r ON r.NAME=a.NAME AND r.BLOCK_TIMESTAMP=t.BLOCK_TIMESTAMP",h)
  b=dbGetQuery(con,qq)
  
  dbSendUpdate(con,"DROP TABLE IF EXISTS RXTALIASTMP")
  
  bigint.cols = c("OWNER_ID","TRANSACTION_ID")
  for(i in bigint.cols) b[,i] = nxt.convert.id(b[,i],from.db=id.from.db)
  
  b$BLOCK_TIMESTAMP = nxt.convert.ts(b$BLOCK_TIMESTAMP,from.db=ts.from.db)
  b$FIRST_ASSIGN = nxt.convert.ts(b$FIRST_ASSIGN,from.db=ts.from.db)
  
  row.names(b)=as.character(b$NAME)
  
  return(b)
}
