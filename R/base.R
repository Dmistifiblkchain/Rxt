#' Timestamp of NXT genesis block in POSIXct format
#'   
#' @seealso \code{\link{nxt.convert.ts}}
#' 
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @export
nxt.genesis.ts <- as.POSIXct("2013-11-24 12:00:00",tz="UTC")

#' Test if an object is of class POSIXct or POSIXlt or POSIXt
#' 
#' @param x Any R object
#'   
#' @return Returns TRUE if object is of class POSIXct or POSIXlt or POSIXt
#'   
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @keywords internal
is.POSIXt <- function(x)
  length(grep("^POSIX[lc]?t",class(x)))>0

#' Convert timestamps back and forth between seconds since genesis and POSIXct
#' 
#' Timestamps are stored internally in the NXT H2 database in units of seconds 
#' since the genesis block. This function converts back and forth between the 
#' internal format and POSIXct timestamp objects. If the function is not 
#' explicitly told which direction to convert to it will try to automatically 
#' determine the correct direction based on inputs.
#' 
#' @param ts a vector of timestamp values.  Can be numeric, in which case values
#'   are assumed to reprsent seconds since genesis block; POSIX timestamp 
#'   objects; or character, which is converted to POSIXct
#' @param tz timezone to use when converting character to POSIXct.  Defaults to 
#'   "UTC"
#' @param from.db Boolean. If true, try to convert \code{ts} into POSIXct. If 
#'   false, tries to convert into seconds since the genesis block. By default, 
#'   will try to automatically convert numeric into POSIXct and all others into 
#'   numeric (seconds since genesis).
#' @param genesis.ts timestamp of genesis block in POSIXct format. Defaults to
#'   \code{\link{nxt.genesis.ts}}.
#'   
#' @return A vector of timestamps in the desired format.
#'   
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @export
nxt.convert.ts <- function(ts,tz="UTC",from.db=length(grep("^(character|POSIX[lc]?t)",class(ts)))==0,genesis.ts=nxt.genesis.ts) {

  # The above default for from.db will try to automatically determine which direction we want to convert
  # If class is a POSIXlt or POSIXt or POSIXct, then assume we want to convert to seconds since genesis
  # If not, then assume we have been given seconds and want to convert to a POSIXct object

  # If not numeric or POSIXt, then assume that these are timestamp strings and convert to POSIXct
  if ( !is.numeric(ts) & !is.POSIXt(ts) )
    ts = as.POSIXct(ts,tz=tz)
  
  if (from.db) {
    # Only convert to POSIXct if given numeric input
    # This will leave untouched other inputs
    if (is.numeric(ts))
      ts = genesis.ts+ts
  } else {
    if (is.POSIXt(ts))
      ts = as.numeric(ts-genesis.ts,units="secs")
  }
  
  return(ts)
}

#' Convert NXT IDs between internal H2 DB format and canonical unsigned long 
#' format
#' 
#' NXT IDs (e.g., for accounts or transactions or blocks) are stored internally 
#' in the NXT H2 database as signed long. When querying the database, these are 
#' converted to strings to avoid loss of precision, but IDs are not converted by
#' the database to the canonical unsigned long format used by NXT. This function
#' converts IDs back and forth between unsigned and signed long using 
#' \code{\link[gmp]{bigz}} objects from the gmp package.
#' 
#' For efficiency, results are always returned as character strings.
#' 
#' @param id a vector of NXT ids.  Can be of any type that can be converted to
#'   \code{\link[gmp]{bigz}}.
#' @param from.db Boolean. If true, try to convert \code{id} to canonical
#'   unsigned long NXT ID format. If false, converts from unsigned long to
#'   signed long. By default, will try to automatically convert from the
#'   presumed input format to the other.
#'   
#' @return A vector of character strings containing the IDs in the desired format.
#'   
#' @seealso \code{\link[gmp]{bigz}}
#'   
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @export
#' @import gmp
nxt.convert.id <- function(id,from.db=any(as.bigz(id)<0)) {
  
  # The above default for from.db will try to automatically determine which direction we want to convert
  # If negative values, assume we want to go to standard NXT ID format
  # Otherwise, subtract 2^64 from any values larger than 2^63 (i.e., go to DB ID format)

  #require(gmp)
  n = as.bigz(2^63)
  id = as.bigz(id)

  # Protect against fatal crash referencing empty bigz object
  if (length(id)==0) return(id)
  
  if (from.db) {
    I = !is.na(id) & id<0 # Careful with NA that occurs when BLOCK_ID is empty
    id[I]=id[I] + 2*n
  } else {
    I = !is.na(id) & id>n
    id[I] = id[I] - 2*n
  }
  
  return(as.character(id))
}

#' Make a connection to the NXT H2 database containing the blockchain
#' 
#' Recent versions of the NRS NXT client store the blockchain in a H2 database. 
#' This function facilitates connecting to this database using the RH2 package.
#' This requires RH2 >= 0.2 and NRS client >= 0.8. If you want to connect to the
#' NXT H2 database while running the NRS client, then one must instruct the NRS
#' client to open the database with the AUTO_SERVER=TRUE option using the
#' 'nxt.dbUrl' config property.
#' 
#' @param file Full path to the file containing the NXT H2 database.
#' @param H2.opts Configuration options for the H2 connection.  Defaults to
#'   ";DB_CLOSE_ON_EXIT=FALSE;AUTO_SERVER=TRUE".
#' @param H2.prefix In general, don't touch. Defaults to "jdbc:h2:".
#' @param username In general, don't touch. Defaults to "sa".
#' @param password In general, don't touch. Defaults to "sa".
#' @param \dots Further arguments to the \code{\link[RH2]{H2}} driver function
#'   (e.g., jars).
#'   
#' @return An H2 DB connection object
#'   
#' @seealso \code{\link{nxt.dbDisconnect}}, \code{\link[RH2]{H2}}
#'   
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @export
#' @import RH2
nxt.dbConnect <- function(file="nxt/nxt_db/nxt.h2.db",H2.opts=";DB_CLOSE_ON_EXIT=FALSE;AUTO_SERVER=TRUE",
                         H2.prefix="jdbc:h2:",username="sa",password="sa",...) {
  #require(RH2)
  
  # Remove possible file extension
  file=sub("[.]h2[.]db$","",file) 
  
  # Generatre DB URI
  uri=paste(H2.prefix,paste(file,H2.opts,sep=""),sep="")
  
  # Make connection
  return(dbConnect(H2(...),uri,username,password))
}


#' Disconnect from NXT H2 database
#' 
#' Currently just a wrapper for \code{[\link[DBI]{dbDisconnect}}.
#' 
#' @param con H2 connection object
#'   
#' @return Boolean indicating success of operation
#'   
#' @seealso \code{\link{nxt.dbConnect}}
#'   
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @export
nxt.dbDisconnect <- function(con)
  dbDisconnect(con)

#' Generate a timeseries of creation of new NXT accounts
#' 
#' This function returns a timeseries of the number of new NXT accounts created 
#' on each timestep since the genesis block.  Timestep is controlable, but 
#' defaults to daily.
#' 
#' @param con Connection object to the NXT H2 database
#' @param timestep Size of timestep to use for generating timeseries.  Can be a 
#'   number of seconds for each timestep or "daily" (default), "weekly", 
#'   "monthly" or "yearly".
#' @param ts.from.db Boolean. If \code{TRUE} (default), convert timestamps to POSIXct,
#'   otherwise keep them in seconds since genesis block.
#'   
#' @return A data.frame with the following columns:
#' \item{TIMESTAMP}{Timestamp at the start of the timestep}
#'
#' \item{N_ACCOUNT}{Total number of NXT accounts existing at the end of the timestep}
#'   
#' \item{D_ACCOUNT}{Number of new NXT accounts created during the timestep}
#'   
#' @seealso \code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}} 
#'   
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @export
nxt.newAccountsTimeSeries <- function(con,timestep="daily",ts.from.db=TRUE) {
  if (is.character(timestep)) {
    hr=60*60
    timestep=c(hourly=hr,daily=24*hr,weekly=7*24*hr,monthly=30*24*hr,yearly=365*24*hr)[timestep]    
  }

  q = paste("
SELECT d.TS AS TIMESTAMP, count(DISTINCT t.RECIPIENT_ID) AS N_ACCOUNT
FROM
(
SELECT DISTINCT TIMESTAMP - (TIMESTAMP % (",format(timestep,digits=20),")) AS TS
FROM PUBLIC.BLOCK
GROUP BY TS
) AS d JOIN PUBLIC.TRANSACTION AS t ON t.TIMESTAMP<=d.TS
GROUP BY TIMESTAMP
ORDER BY TIMESTAMP
")
  
  newact=dbGetQuery(con,q)
  
  # Fix weird bug of misnamed column
  names(newact)[names(newact)=="TS"]="TIMESTAMP"
  
  if (ts.from.db)
    newact$TIMESTAMP = nxt.convert.ts(newact$TIMESTAMP)
  
  newact$D_ACCOUNT = c(0,diff(newact$N_ACCOUNT))
  
  return(newact)  
}

#' Get a set of blocks from the NXT blockchain
#' 
#' This function queries the NXT blockchain and returns a set of blocks meeting 
#' a certain set of criteria.
#' 
#' @param con Connection object to the NXT H2 database.
#' @param block.ids A vector of NXT block IDs to query for.
#' @param generator.ids A vector of NXT account IDs to look for as the 
#'   generators of the blocks.
#' @param start.ts Minimum timestamp of block. Can be in seconds since genesis 
#'   or POSIXct format.
#' @param end.ts Maximum timestamp of block. Can be in seconds since genesis or 
#'   POSIXct format.
#' @param start.height Minimum block height.
#' @param end.height Maximum block height.
#' @param nonzero.fee Boolean. Defaults to \code{FALSE}. If \code{TRUE}, only 
#'   return blocks that had transactions in them.
#' @param ts.from.db Boolean. If \code{TRUE} (default), convert timestamps to 
#'   POSIXct, otherwise keep them in seconds since genesis block.
#' @param id.from.db Boolean. If \code{TRUE} (default), output block and account
#'   IDs in canonical format, otherwise leave in signed long format.
#'   
#' @return A data.frame with exactly the same information as is found in the 
#'   BLOCK table found in the NXT H2 database.
#'   
#' @seealso \code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}},
#'   \code{\link{nxt.convert.id}}
#'   
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @export
nxt.getBlocks <- function(con,block.ids=NULL,generator.ids=NULL,start.ts=NULL,end.ts=NULL,
                          start.height=NULL,end.height=NULL,nonzero.fee=FALSE,
                          ts.from.db=TRUE,id.from.db=TRUE) {
  w="WHERE TRUE"
  
  if (!is.null(block.ids)) {
    block.ids=paste(nxt.convert.id(block.ids,from.db=FALSE),collapse=",")
    w=paste(w," AND ID IN (",block.ids,")",sep="")
  }
  
  if (!is.null(generator.ids)) {
    block.ids=paste(nxt.convert.id(generator.ids,from.db=FALSE),collapse=",")
    w=paste(w," AND ID IN (",block.ids,")",sep="")
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
  
  if (nonzero.fee) {
    w=paste(w,"AND TOTAL_FEE>0")
  }
  
  b=dbGetQuery(con,paste("SELECT DB_ID, CAST(ID AS VARCHAR) AS ID,
                         VERSION, TIMESTAMP, 
                         CAST(PREVIOUS_BLOCK_ID AS VARCHAR) AS PREVIOUS_BLOCK_ID,
                         TOTAL_AMOUNT, TOTAL_FEE, PAYLOAD_LENGTH, GENERATOR_PUBLIC_KEY,
                         PREVIOUS_BLOCK_HASH, CUMULATIVE_DIFFICULTY, 
                         CAST(BASE_TARGET AS VARCHAR) AS BASE_TARGET,
                         CAST(NEXT_BLOCK_ID AS VARCHAR) AS NEXT_BLOCK_ID,
                         HEIGHT, GENERATION_SIGNATURE, BLOCK_SIGNATURE, PAYLOAD_HASH,
                         CAST(GENERATOR_ID AS VARCHAR) AS GENERATOR_ID
                         FROM PUBLIC.BLOCK",w))
  
  # Change factors back into character strings - there must be a way to avoid conversion to factor
  I = sapply(b,class)=="factor"
  b[,I] = sapply(b[,I],as.character)
  
  b$TIMESTAMP = nxt.convert.ts(b$TIMESTAMP,from.db=ts.from.db)

  id.cols = c("ID","PREVIOUS_BLOCK_ID","NEXT_BLOCK_ID","GENERATOR_ID")
  for(i in id.cols) b[,i] = nxt.convert.id(b[,i],from.db=id.from.db)

  row.names(b)=as.character(b$ID)
  
  return(b)
}

#' Get last block from the NXT blockchain
#' 
#' This function queries the NXT blockchain and returns the most recent block.
#' 
#' @param con Connection object to the NXT H2 database.
#' @param nonzero.fee Boolean. Defaults to \code{FALSE}. If \code{TRUE}, get
#'   last block that had atleast one transaction.
#' @param \dots Additional arguments for \code{\link{nxt.getBlocks}}, such as 
#'   \code{ts.from.db} and \code{id.from.db}
#'   
#' @return A data.frame with exactly the same information as is found in the 
#'   BLOCK table found in the NXT H2 database.
#'   
#' @seealso \code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}}, 
#'   \code{\link{nxt.convert.id}}, \code{\link{nxt.getBlocks}}
#'   
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @export
nxt.getLastBlock <- function(con,nonzero.fee=FALSE,...) {
  q="SELECT max(HEIGHT) FROM PUBLIC.BLOCK"
  if (nonzero.fee)
    paste(q,"WHERE TOTAL_FEE>0")
  
  h=dbGetQuery(con,q)
  return(nxt.getBlocks(con,start.height=h,end.height=h,...))
}

#' Get a set of transactions from the NXT blockchain
#' 
#' This function queries the NXT blockchain and returns a set of transactions 
#' meeting a certain set of criteria.
#' 
#' @param con Connection object to the NXT H2 database.
#' @param block.ids A vector of NXT block IDs to query for.
#' @param sender.ids A vector with NXT IDs of senders of transactions.
#' @param recipient.ids A vector with NXT IDs of recipients of transactions.
#' @param start.ts Minimum timestamp of transaction. Can be in seconds since
#'   genesis or POSIXct format.
#' @param end.ts Maximum timestamp of transaction. Can be in seconds since
#'   genesis or POSIXct format.
#' @param start.height Minimum height of block corresponding to transaction.
#' @param end.height Maximum height of block corresponding to transaction.
#' @param types Transaction types to look for (e.g., 0 for payment, 1 for
#'   messaging, etc.)
#' @param subtypes Transaction subtypes to look for (e.g., types=1, subtypes=1
#'   for alias assignments)
#' @param min.amount Minimum transaction amount (in NXT).
#' @param max.amount Maximum transaction amount (in NXT).
#' @param min.fee Minimum transaction fee (in NXT).
#' @param max.fee Maximum transaction fee (in NXT).
#' @param ts.from.db Boolean. If \code{TRUE} (default), convert timestamps to 
#'   POSIXct, otherwise keep them in seconds since genesis block.
#' @param id.from.db Boolean. If \code{TRUE} (default), output block and account
#'   IDs in canonical NXT format, otherwise leave in signed long format.
#'   
#' @return A data.frame with exactly the same information as is found in the 
#'   TRANSACTION table found in the NXT H2 database.
#'   
#' @seealso \code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}}, 
#'   \code{\link{nxt.convert.id}}
#'   
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @export
nxt.getTransactions <- function(con,block.ids=NULL,sender.ids=NULL,recipient.ids=NULL,
                                start.ts=NULL,end.ts=NULL,
                                start.height=NULL,end.height=NULL,
                                types=NULL,subtypes=NULL,
                                min.amount=NULL,max.amount=NULL,
                                min.fee=NULL,max.fee=NULL,
                                ts.from.db=TRUE,id.from.db=TRUE) {
  w="WHERE TRUE"
  
  if (!is.null(types)) {
    types=paste(types,collapse=",")
    w=paste(w," AND TYPE IN (",types,")",sep="")
  }
  
  if (!is.null(subtypes)) {
    subtypes=paste(subtypes,collapse=",")
    w=paste(w," AND SUBTYPE IN (",subtypes,")",sep="")
  }
  
  if (!is.null(block.ids)) {
    block.ids=paste(nxt.convert.id(block.ids,from.db=FALSE),collapse=",")
    w=paste(w," AND BLOCK_ID IN (",block.ids,")",sep="")
  }
  
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
  
  if (!is.null(min.amount)) {
    w=paste(w," AND AMOUNT>=",min.amount,sep="")
  }
  
  if (!is.null(max.amount)) {
    w=paste(w," AND AMOUNT<=",max.amount,sep="")
  }
  
  if (!is.null(min.fee)) {
    w=paste(w," AND FEE>=",min.fee,sep="")
  }
  
  if (!is.null(max.fee)) {
    w=paste(w," AND FEE<=",max.fee,sep="")
  }

  b=dbGetQuery(con,paste("SELECT DB_ID, CAST(ID AS VARCHAR) AS ID,
                         DEADLINE, SENDER_PUBLIC_KEY, 
                         CAST(RECIPIENT_ID AS VARCHAR) AS RECIPIENT_ID,
                         AMOUNT, FEE,
                         CAST(REFERENCED_TRANSACTION_ID AS VARCHAR) AS REFERENCED_TRANSACTION_ID,
                         HEIGHT, 
                         CAST(BLOCK_ID AS VARCHAR) AS BLOCK_ID,
                         SIGNATURE, TIMESTAMP, TYPE, SUBTYPE,
                         CAST(SENDER_ID AS VARCHAR) AS SENDER_ID,
                         ATTACHMENT
                         FROM PUBLIC.TRANSACTION",w))

  # Change factors back into character strings - there must be a way to avoid conversion to factor
  I = sapply(b,class)=="factor"
  b[,I] = sapply(b[,I],as.character)
  
  b$TIMESTAMP = nxt.convert.ts(b$TIMESTAMP,from.db=ts.from.db)
  
  id.cols = c("ID","RECIPIENT_ID","SENDER_ID","REFERENCED_TRANSACTION_ID","BLOCK_ID")
  for(i in id.cols) b[,i] = nxt.convert.id(b[,i],from.db=id.from.db)
  
  row.names(b)=as.character(b$ID)
  
  return(b)
}

#' Get balances for a set of NXT accounts
#' 
#' This function queries the NXT blockchain and returns the NXT balance for a
#' set of account IDs at a specifief time.
#' 
#' If an account ID is not found, it will not appear in the final data.frame.
#' 
#' @param con Connection object to the NXT H2 database.
#' @param account.ids A vector with NXT IDs of accounts to calculate balances 
#'   for.
#' @param ts Timestamp at which to calculate balances. Can be in seconds since 
#'   genesis or POSIXct format. Defaults to calculating balance over all 
#'   transactions.
#' @param id.from.db Boolean. If \code{TRUE} (default), output account IDs in 
#'   canonical NXT format, otherwise leave in signed long format.
#'   
#' @return A data.frame with two columns: ACCOUNT_ID and BALANCE
#'   
#' @seealso \code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.id}}
#'   
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @export
nxt.getBalances <- function(con,account.ids=NULL,ts=NULL,id.from.db=TRUE) {
  w="WHERE TRUE"
  
  if (!is.null(ts)) {
    ts=nxt.convert.ts(ts,from.db=FALSE)
    w=paste(w," AND TIMESTAMP<=",ts,sep="")
  }
  
  i=c("SENDER_ID","RECIPIENT_ID","GENERATOR_ID")
  if (!is.null(account.ids)) {
    ids=paste(nxt.convert.id(account.ids,from.db=FALSE),collapse=",")
    w=paste(w,"AND",i,"IN (",ids,")")
  }
  
  a=c("-1*SUM(AMOUNT+FEE)","SUM(AMOUNT)","SUM(TOTAL_FEE)")
  t=c("PUBLIC.TRANSACTION","PUBLIC.TRANSACTION","PUBLIC.BLOCK")
  
  tt=paste("nxtbalancetemp",1:3,sep="")
  q=paste("CREATE LOCAL TEMPORARY TABLE",tt,"AS SELECT",i,"AS ID,",a,"AS AMOUNT FROM",t,w,"GROUP BY ID")
  
  for (n in 1:length(q)) {
    dbSendUpdate(con,paste("DROP TABLE IF EXISTS",tt[n]))
    dbSendUpdate(con,q[n])
  }
  
  qq = paste("SELECT CAST(ID AS VARCHAR) AS ACCOUNT_ID, SUM(AMOUNT) AS BALANCE FROM (",
             paste(paste("SELECT * FROM",tt),collapse=" UNION "),") t GROUP BY ID")

  b=dbGetQuery(con,qq)

  for (ttt in tt) {
    dbSendUpdate(con,paste("DROP TABLE",ttt))
  }
  
  b$ACCOUNT_ID=nxt.convert.id(as.character(b$ACCOUNT_ID),from.db=id.from.db)
  
  if (is.null(account.ids)) {
    row.names(b)=b$ACCOUNT_ID  
    return(b)
  }
  
  account.ids=nxt.convert.id(account.ids,from.db=id.from.db)
  cc=data.frame(ACCOUNT_ID=as.character(account.ids),BALANCE=0,
                stringsAsFactors=FALSE)
  row.names(cc)=account.ids
  cc[b$ACCOUNT_ID,"BALANCE"]=b$BALANCE
  
  return(cc)
}

#' Get a timeseries of transactions for a given set of account IDs
#' 
#' This function queries the NXT blockchain and returns an list of transactions,
#' ordered by timestamp, including information about the type of transaction, 
#' amounts, etc. for a set of account IDs.
#' 
#' @param con Connection object to the NXT H2 database.
#' @param account.ids A vector with NXT IDs of accounts to look for.
#' @param start.ts Minimum timestamp of transactions. Can be in seconds since 
#'   genesis or POSIXct format.
#' @param end.ts Maximum timestamp of transactions. Can be in seconds since 
#'   genesis or POSIXct format.
#' @param ts.from.db Boolean. If \code{TRUE} (default), convert timestamps to 
#'   POSIXct, otherwise keep them in seconds since genesis block.
#' @param id.from.db Boolean. If \code{TRUE} (default), output block and account
#'   IDs in canonical NXT format, otherwise leave in signed long format.
#' @param calc.balance Boolean. If \code{TRUE} (default), a running balance will
#'   be calculated for all transactions. This will be the balance for all 
#'   \code{account.ids} and will only reflect transactions occuring between 
#'   \code{start.ts} and \code{end.ts}.
#'   
#' @return A data.frame with the following columns: \item{ACCOUNT_ID}{NXT 
#'   account ID from list given in \code{account.ids}}
#'   
#'   \item{TIMESTAMP}{Timestamp of transaction}
#'   
#'   \item{TRANSACTION_ID}{NXT ID of transaction}
#'   
#'   \item{TRANSACTION_DIRECTION}{"SEND", "RECEIVE", or "FORGE"}
#'   
#'   \item{TRANSACTION_TYPE}{Integer indicating transaction type}
#'   
#'   \item{TRANSACTION_SUBTYPE}{Integer indicating transaction subtype}
#'   
#'   \item{OTHER_ACCOUNT_ID}{Any other NXT account ID relevant to transaction}
#'   
#'   \item{AMOUNT}{NXT amount of transaction with sign indicating impact on 
#'   ACCOUNT_ID (e.g., negative for send transactions)}
#'   
#'   \item{FEE}{NXT fee for transaction with sign indicating impact on 
#'   ACCOUNT_ID (e.g., positive for block forgine)}
#'   
#'   \item{BALANCE}{Cumsum of \code{AMOUNT+FEE} for all transaction (only if
#'   \code{calc.balance=TRUE})}
#'   
#' @seealso \code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}}, 
#'   \code{\link{nxt.convert.id}}
#'   
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @export
nxt.getAccountsTimeSeries <- function(con,account.ids,start.ts=NULL,end.ts=NULL,
                                      ts.from.db=TRUE,id.from.db=TRUE,calc.balance=TRUE) {
  i=c("SENDER_ID","RECIPIENT_ID","GENERATOR_ID")
  t=c("PUBLIC.TRANSACTION","PUBLIC.TRANSACTION","PUBLIC.BLOCK")
  a=c("AMOUNT","AMOUNT","0")
  f=c("FEE","0","TOTAL_FEE")
  as=c(-1,1,1)
  fs=c(-1,1,1)
  tt=c("'SEND'","'RECEIVE'","'FORGE'")
  tty=c("TYPE","TYPE","NULL")
  tsty=c("SUBTYPE","SUBTYPE","NULL")
  o=c("RECIPIENT_ID","SENDER_ID","NULL")
  
  w=paste("WHERE",i,"IN (",
          paste(nxt.convert.id(account.ids,from.db=FALSE),collapse=","),")")
  
  if (!is.null(start.ts)) {
    start.ts=nxt.convert.ts(start.ts,from.db=FALSE)
    w=paste(w," AND TIMESTAMP>=",start.ts,sep="")
  }
  
  if (!is.null(end.ts)) {
    end.ts=nxt.convert.ts(end.ts,from.db=FALSE)
    w=paste(w," AND TIMESTAMP<=",end.ts,sep="")
  }
  
  tempt=paste("nxatstemp",1:3,sep="")
  q=paste("CREATE LOCAL TEMPORARY TABLE",tempt,
          "AS SELECT",i,"AS ACCOUNT_ID, TIMESTAMP, ID AS TRANSACTION_ID,",
          tt,"AS TRANSACTION_DIRECTION,",
          tty,"AS TRANSACTION_TYPE,",
          tsty,"AS TRANSACTION_SUBTYPE,",
          o,"AS OTHER_ACCOUNT_ID,",
          as,"*",a,"AS AMOUNT,",
          fs,"*",f,"AS FEE FROM",t,w)

  for (n in 1:length(q)) {
    dbSendUpdate(con,paste("DROP TABLE IF EXISTS",tempt[n]))
    dbSendUpdate(con,q[n])
  }
  
  qq = paste("SELECT CAST(ACCOUNT_ID AS VARCHAR) AS ACCOUNT_ID, TIMESTAMP,",
             "CAST(TRANSACTION_ID AS VARCHAR) AS TRANSACTION_ID, TRANSACTION_DIRECTION,",
             "TRANSACTION_TYPE, TRANSACTION_SUBTYPE,",
             "CAST(OTHER_ACCOUNT_ID AS VARCHAR) AS OTHER_ACCOUNT_ID,",
             " AMOUNT, FEE FROM (",
             paste(paste("SELECT * FROM",tempt),collapse=" UNION "),
             ") AS t ORDER BY TIMESTAMP, ACCOUNT_ID")

  b = dbGetQuery(con,qq)

  for (ttt in tempt) {
    dbSendUpdate(con,paste("DROP TABLE",ttt))
  }
  
  # Change factors back into character strings - there must be a way to avoid conversion to factor
  I = sapply(b,class)=="factor"
  b[,I] = sapply(b[,I],as.character)

  b$TIMESTAMP = nxt.convert.ts(b$TIMESTAMP,from.db=ts.from.db)
  
  id.cols = c("ACCOUNT_ID","TRANSACTION_ID","OTHER_ACCOUNT_ID")
  for(i in id.cols) b[,i] = nxt.convert.id(b[,i],from.db=id.from.db)
  
  if (calc.balance)
    b$BALANCE=cumsum(b$AMOUNT+b$FEE)
  
  return(b)
}

#' Get summary account activity statistics for a set of NXT accounts
#' 
#' This function queries the NXT blockchain and returns summary statistics 
#' regarding transactions and forging for a set of NXT accounts for a given time
#' period.
#' 
#' @param con Connection object to the NXT H2 database.
#' @param account.ids A vector with NXT IDs of accounts to look for. Defaults to
#'   calculating statistics for all accounts.
#' @param start.ts Minimum timestamp of transactions and blocks. Can be in 
#'   seconds since genesis or POSIXct format. Defaults to start of NXT 
#'   blockchain.
#' @param end.ts Maximum timestamp of transactions and blocks. Can be in seconds
#'   since genesis or POSIXct format. Defaults to end of NXT blockchain.
#' @param ts.from.db Boolean. If \code{TRUE} (default), convert timestamps to 
#'   POSIXct, otherwise keep them in seconds since genesis block.
#' @param id.from.db Boolean. If \code{TRUE} (default), output block and account
#'   IDs in canonical NXT format, otherwise leave in signed long format.
#' @param calc.balance Boolean. If \code{TRUE} (default), a balance will be 
#'   calculated for each account. This balance will only reflect transactions 
#'   occuring between \code{start.ts} and \code{end.ts}.
#'   
#' @return A data.frame with the following columns corresponding to statistics 
#'   for a given set of account IDs over a given time period: 
#'   \item{ACCOUNT_ID}{NXT account ID from list given in \code{account.ids}}
#'   
#'   \item{N_REC}{Number of times account received NXT}
#'   
#'   \item{N_MESS_REC}{Number of messages received by account}
#'   
#'   \item{N_MESS_SENDERS}{Number of distinct accounts sending messages to this 
#'   account}
#'   
#'   \item{NXT_REC}{Amount of NXT sent to this account}
#'   
#'   \item{FIRST_REC}{Timestamp of first transaction received by account}
#'   
#'   \item{LAST_REC}{Timestamp of last NXT transfer to this account}
#'   
#'   \item{N_TRANS}{Total number of transactions of all types sent by this 
#'   account}
#'   
#'   \item{FEE_PAID}{Total fee paid by this count for sending transactions}
#'   
#'   \item{NXT_SENT}{Amount of NXT sent from this account}
#'   
#'   \item{FIRST_SEND}{Timestamp of first time NXT sent from this account}
#'   
#'   \item{LAST_SEND}{Timestamp of last time NXT sent from this account}
#'   
#'   \item{N_SEND}{Number of times NXT sent from this account}
#'   
#'   \item{N_MESS_SENT}{Number of messages sent from this account}
#'   
#'   \item{N_MESS_RECIPIENTS}{Number of distinct message recipients among 
#'   messages sent from this account}
#'   
#'   \item{N_ALIAS_ASSIGNS}{Number of alias assignment transactions sent by this
#'   account. Not necessarily all alias transactions correspond to different NXT
#'   aliases}
#'   
#'   \item{N_COLORED_TRANS}{Number of colored coin transactions sent by this 
#'   account}
#'   
#'   \item{N_FORGED}{Number of blocks forged by this account}
#'   
#'   \item{NONZERO_N_FORGED}{Number of blocks forged by this account with 
#'   nonzero fee}
#'   
#'   \item{FEE_FORGED}{Amount of NXT forged by this account}
#'   
#'   \item{FIRST_FORGED}{Timestamp of first block forged by this account}
#'   
#'   \item{LAST_FORGED}{Timestamp of last block forged by this account}
#'   
#'   \item{NONZERO_FIRST_FORGED}{Timestamp of first block forged by this account
#'   with nonzero fee}
#'   
#'   \item{NONZERO_LAST_FORGED}{Timestamp of last block forged by this account 
#'   with nonzero fee}
#'   
#'   \item{FIRST_ACT}{Timestamp of first account activity}
#'   
#'   \item{LAST_ACT}{Timestamp of first account activity}
#'   
#'   \item{TIME_ACT}{Difference between \code{FIRST_ACT} and \code{LAST_ACT}}
#'   
#'   \item{BALANCE}{Balance of account for time period examined (only if 
#'   \code{calc.balance=TRUE})}
#'   
#' @seealso \code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}}, 
#'   \code{\link{nxt.convert.id}}
#'   
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @export
nxt.getAccountsStats <- function(con,account.ids=NULL,start.ts=NULL,end.ts=NULL,
                                 ts.from.db=TRUE,id.from.db=TRUE,calc.balance=TRUE) {

  w = rep("WHERE TRUE",3)
  
  ii=c("SENDER_ID","RECIPIENT_ID","GENERATOR_ID")
  if (!is.null(account.ids)) {
    a=paste(nxt.convert.id(account.ids,from.db=FALSE),collapse=",")
    w=paste(w,"AND",ii,"IN (",a,")")
  }
  
  if (!is.null(start.ts)) {
    start.ts=nxt.convert.ts(start.ts,from.db=FALSE)
    w=paste(w," AND TIMESTAMP>=",start.ts,sep="")
  }
  
  if (!is.null(end.ts)) {
    end.ts=nxt.convert.ts(end.ts,from.db=FALSE)
    w=paste(w," AND TIMESTAMP<=",end.ts,sep="")
  }
  
  q=c(
    "SELECT SENDER_ID AS ACCOUNT_ID, 
       count(*) AS N_TRANS,
    sum(AMOUNT) AS NXT_SENT, 
    sum(FEE) AS FEE_PAID,
    min(TIMESTAMP) AS FIRST_SEND,
    max(TIMESTAMP) AS LAST_SEND,
    sum(TYPE=0) AS N_SEND,
    sum(TYPE=1 AND SUBTYPE=0) AS N_MESS_SENT,
    count(DISTINCT (CASE WHEN TYPE=1 AND SUBTYPE=0 THEN RECIPIENT_ID ELSE NULL END)) AS N_MESS_RECIPIENTS,
    sum(TYPE=1 AND SUBTYPE=1) AS N_ALIAS_ASSIGNS,
    sum(TYPE=2) AS N_COLORED_TRANS
    FROM PUBLIC.TRANSACTION",
      "SELECT RECIPIENT_ID AS ACCOUNT_ID,
    sum(TYPE=0) AS N_REC,
    sum(TYPE=1 AND SUBTYPE=0) AS N_MESS_REC,
    count(DISTINCT (CASE WHEN TYPE=1 AND SUBTYPE=0 THEN SENDER_ID ELSE NULL END)) AS N_MESS_SENDERS,
    sum(AMOUNT) AS NXT_REC,
    min(TIMESTAMP) AS FIRST_REC,
    max(TIMESTAMP) AS LAST_REC
    FROM PUBLIC.TRANSACTION",
      "SELECT GENERATOR_ID AS ACCOUNT_ID,
    count(*) AS N_FORGED,
    sum(TOTAL_FEE>0) AS NONZERO_N_FORGED,
    sum(TOTAL_FEE) AS FEE_FORGED,
    min(TIMESTAMP) AS FIRST_FORGED,
    max(TIMESTAMP) AS LAST_FORGED,               
    min(CASE WHEN TOTAL_FEE>0 THEN TIMESTAMP ELSE NULL END) AS NONZERO_FIRST_FORGED,
    max(CASE WHEN TOTAL_FEE>0 THEN TIMESTAMP ELSE NULL END) AS NONZERO_LAST_FORGED
    FROM PUBLIC.BLOCK"      
    )

  tt = paste("RXTGAS",1:3,sep="")
  q=paste("CREATE LOCAL TEMPORARY TABLE",tt,"AS",q,w,"GROUP BY",ii)

  ttix=paste(tt,"_IX",sep="")
  q2=paste("CREATE UNIQUE INDEX",ttix,"ON",tt,"(ACCOUNT_ID)")
  
  for (n in 1:length(q)) {
    dbSendUpdate(con,paste("DROP TABLE IF EXISTS",tt[n]))
    dbSendUpdate(con,q[n])
    dbSendUpdate(con,q2[n])
  }

  qq=paste(
    "SELECT CAST(r.ACCOUNT_ID AS VARCHAR) AS ACCOUNT_ID,
    N_REC, N_MESS_REC, N_MESS_SENDERS, COALESCE(NXT_REC,0) AS NXT_REC, 
    FIRST_REC, LAST_REC, N_TRANS, 
    COALESCE(FEE_PAID,0) AS FEE_PAID, COALESCE(NXT_SENT,0) AS NXT_SENT,  
    FIRST_SEND, LAST_SEND,
    N_SEND, N_MESS_SENT, N_MESS_RECIPIENTS, N_ALIAS_ASSIGNS, N_COLORED_TRANS,
    N_FORGED, NONZERO_N_FORGED, COALESCE(FEE_FORGED,0) AS FEE_FORGED, 
    FIRST_FORGED, 
    LAST_FORGED,
    NONZERO_FIRST_FORGED, 
    NONZERO_LAST_FORGED
    FROM",tt[2],"AS r LEFT JOIN",tt[1],"AS s ON r.ACCOUNT_ID=s.ACCOUNT_ID LEFT JOIN",
    tt[3],"AS g ON r.ACCOUNT_ID=g.ACCOUNT_ID ORDER BY r.ACCOUNT_ID
    ")

  b = dbGetQuery(con,qq)
  
  for (n in 1:length(q)) {
    dbSendUpdate(con,paste("DROP TABLE IF EXISTS",tt[n]))
  }

  # Change factors back into character strings - there must be a way to avoid conversion to factor
  I = sapply(b,class)=="factor"
  b[,I] = sapply(b[,I],as.character)
  
  b$FIRST_ACT = apply(b[,c("FIRST_REC","FIRST_SEND","FIRST_FORGED")],1,min,na.rm=TRUE)
  b$LAST_ACT = apply(b[,c("LAST_REC","LAST_SEND","LAST_FORGED")],1,min,na.rm=TRUE)

  if (ts.from.db) {
    ts.cols = c("FIRST_REC","LAST_REC","FIRST_SEND","LAST_SEND",
                "FIRST_FORGED","LAST_FORGED","FIRST_ACT","LAST_ACT",
                "NONZERO_FIRST_FORGED","NONZERO_LAST_FORGED"
                )
    for(i in ts.cols) b[,i] = nxt.convert.ts(b[,i],from.db=TRUE)
  }
  b$TIME_ACT = b$LAST_ACT - b$FIRST_ACT # Units depends on ts.from.db
  
  b$ACCOUNT_ID = nxt.convert.id(b$ACCOUNT_ID,from.db=id.from.db)
  
  row.names(b)=b$ACCOUNT_ID
  
  if (calc.balance)
    b$BALANCE = b$NXT_REC - b$NXT_SENT - b$FEE_PAID + b$FEE_FORGED
  
  if (!is.null(account.ids) & dim(b)[1]!=length(account.ids))
    warning("Data for some account IDs not found")
  
  return(b)
}

#' Generate a timeseries of NXT transaction activity
#' 
#' This function returns a timeseries of the number of NXT transactions by type,
#' amount of NXT transferred, fees paid, etc. on each timestep since the genesis
#' block.  Timestep is controlable, but defaults to daily.
#' 
#' @param con Connection object to the NXT H2 database
#' @param timestep Size of timestep to use for generating timeseries.  Can be a 
#'   number of seconds for each timestep or "daily" (default), "weekly", 
#'   "monthly" or "yearly".
#' @param ts.from.db Boolean. If \code{TRUE} (default), convert timestamps to 
#'   POSIXct, otherwise keep them in seconds since genesis block.
#'   
#' @return A data.frame with the following columns: \item{TIMESTAMP}{Timestamp 
#'   at the start of the timestep}
#'   
#'   \item{N_TRANS}{Total number of transactions of all types per timestep}
#'   
#'   \item{NXT_SENT}{Amount of NXT sent per timestep}
#'   
#'   \item{FEE_PAID}{Total fees paid per timestep}
#'   
#'   \item{N_SEND}{Number of times NXT sent per timestep}
#'   
#'   \item{N_MESSAGE}{Number of messages sent per timestep}
#'   
#'   \item{N_ALIAS_ASSIGN}{Number of alias assignment transactions per 
#'   timestemp}
#'   
#'   \item{N_COLORED_TRANS}{Number of colored coin transactions per timestep}
#'   
#'   \item{N_DIST_SENDER}{Number of distinct transaction sender accounts}
#'   
#'   \item{N_DIST_RECIP}{Number of distinct transaction recipient accounts}
#'   
#'   \item{N_BLOCK}{Number of blocks per timestep}
#'   
#'   \item{N_BLOCK_FEE}{Number of blocks with fee per timestep}
#'   
#'   \item{N_BLOCK_NOFEE}{Number of blocks without fee per timestep}
#'   
#'   \item{N_DIST_GENER}{Number of distinct block generators per timestep}
#'   
#'   \item{N_DIST_GENER_FEE}{Number of distinct block generators with fee per
#'   timestep}
#'   
#'   \item{MEAN_BASE_TARGET}{Mean of base target}
#'   
#'   \item{MEAN_INV_TARGET}{Mean of 1.0/(base target)}
#'   
#'   \item{MIN_BASE_TARGET}{Minimum base target per timestep}
#'   
#'   \item{MAX_BASE_TARGET}{Maximum base target per timestep}
#'   
#' @seealso \code{\link{nxt.dbConnect}}, \code{\link{nxt.convert.ts}}
#'   
#' @author David M. Kaplan \email{dmkaplan2000@@gmail.com}
#' @export
nxt.getActivityTimeSeries <- function(con,timestep="daily",ts.from.db=TRUE) {
  if (is.character(timestep)) {
    hr=60*60
    timestep=c(hourly=hr,daily=24*hr,weekly=7*24*hr,
               monthly=30*24*hr,yearly=365*24*hr)[timestep]    
  }
  
  q = paste("SELECT COALESCE(t.DATE,b.DATE) AS TIMESTAMP,
              COALESCE(t.N_TRANS,0) AS N_TRANS, 
              COALESCE(t.NXT_SENT,0) AS NXT_SENT, 
              COALESCE(t.FEE_PAID,0) AS FEE_PAID,
              COALESCE(t.N_SEND,0) AS N_SEND, 
              COALESCE(t.N_MESSAGE,0) AS N_MESSAGE, 
              COALESCE(t.N_ALIAS_ASSIGN,0) AS N_ALIAS_ASSIGN, 
              COALESCE(t.N_COLORED_TRANS,0) AS N_COLORED_TRANS,
              COALESCE(t.N_DIST_SENDER,0) AS N_DIST_SENDER, 
              COALESCE(t.N_DIST_RECIP,0) AS N_DIST_RECIP,
              b.N_BLOCK, b.N_BLOCK_FEE, b.N_BLOCK_NOFEE, 
              b.N_DIST_GENER, b.N_DIST_GENER_FEE, b.MEAN_BASE_TARGET, 
              b.MEAN_INV_TARGET, b.MIN_BASE_TARGET, b.MAX_BASE_TARGET
              FROM
                   (
                   SELECT TIMESTAMP - (TIMESTAMP % (",
                    format(timestep,digits=20),")) AS DATE,
                   count(*) AS N_TRANS,
                   sum(AMOUNT) AS NXT_SENT,
                   sum(FEE) AS FEE_PAID,
                   count(DISTINCT SENDER_ID) AS N_DIST_SENDER,
                   count(DISTINCT RECIPIENT_ID) AS N_DIST_RECIP,
                   sum(TYPE=0) AS N_SEND,
                   sum(TYPE=1 AND SUBTYPE=0) AS N_MESSAGE,
                   sum(TYPE=1 AND SUBTYPE=1) AS N_ALIAS_ASSIGN,
                   sum(TYPE=2) AS N_COLORED_TRANS
                   FROM PUBLIC.TRANSACTION
                   GROUP BY DATE
                   ) AS t RIGHT OUTER JOIN 
                   (
                   SELECT TIMESTAMP - (TIMESTAMP % (",
                    format(timestep,digits=20),")) AS DATE,
                   count(*) AS N_BLOCK,
                   sum(TOTAL_FEE>0) AS N_BLOCK_FEE,
                   sum(TOTAL_FEE=0) AS N_BLOCK_NOFEE,
                   count(DISTINCT GENERATOR_ID) AS N_DIST_GENER,
                   count(DISTINCT (CASE WHEN TOTAL_FEE>0 THEN GENERATOR_ID ELSE NULL END)) AS N_DIST_GENER_FEE,
                   AVG(CAST(BASE_TARGET AS DOUBLE)) AS MEAN_BASE_TARGET,
                   AVG(1.0/CAST(BASE_TARGET AS DOUBLE)) AS MEAN_INV_TARGET,
                   MIN(BASE_TARGET) AS MIN_BASE_TARGET,
                   MAX(BASE_TARGET) AS MAX_BASE_TARGET
                   FROM PUBLIC.BLOCK
                   GROUP BY DATE
                   ) AS b ON t.DATE=b.DATE
                   ORDER BY COALESCE(t.DATE,b.DATE)
                   ")

  act=dbGetQuery(con,q)
  
  act$TIMESTAMP = nxt.convert.ts(act$TIMESTAMP,from.db=ts.from.db)
  
  return(act)  
}
