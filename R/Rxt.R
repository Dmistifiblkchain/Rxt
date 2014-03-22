nxt.genesis.ts <- as.POSIXct("2013-11-24 13:00:00",tz="UTC")

is.POSIXt <- function(x)
  length(grep("^POSIX[lc]?t",class(x)))>0

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

nxt.convert.id <- function(id,from.db={require(gmp); any(as.bigz(id)<0)}) {
  
  # The above default for from.db will try to automatically determine which direction we want to convert
  # If negative values, assume we want to go to standard NXT ID format
  # Otherwise, subtract 2^64 from any values larger than 2^63 (i.e., go to DB ID format)

  require(gmp)
  n = as.bigz(2^63)
  id = as.bigz(id)

  if (from.db) {
    id[id<0]=id[id<0] + 2*n
  } else {
    id[id>n] = id[id>n] - 2*n
  }
  
  return(id)
}

nxt.dbConnect <- function(file="nxt/nxt_db/nxt.h2.db",H2.opts=";DB_CLOSE_ON_EXIT=FALSE;AUTO_SERVER=TRUE",
                         H2.prefix="jdbc:h2:",username="sa",password="sa") {
  require(RH2)
  
  # Remove possible file extension
  file=sub("[.]h2[.]db$","",file) 
  
  # Generatre DB URI
  uri=paste(H2.prefix,paste(file,H2.opts,sep=""),sep="")
  
  # Make connection
  return(dbConnect(H2(),uri,username,password))
}

nxt.dbDisconnect <- function(con)
  dbDisconnect(con)

nxt.newAccounts.ts <- function(con,timestep="daily",ts.from.db=TRUE) {
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

nxt.getBlocks <- function(con,block.ids=NULL,generator.ids=NULL,start.ts=NULL,end.ts=NULL,
                          start.height=NULL,end.height=NULL,nonzero.fee=TRUE,
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
  
  # At least convert IDs to BIGZ
  require(gmp)
  bigint.cols = c("ID","PREVIOUS_BLOCK_ID","NEXT_BLOCK_ID","BASE_TARGET","GENERATOR_ID")
  for(i in bigint.cols) b[,i] = as.bigz(b[,i])
  
  if (ts.from.db) {
    b$TIMESTAMP = nxt.convert.ts(b$TIMESTAMP)
  }

  if (id.from.db) {
    id.cols = c("ID","PREVIOUS_BLOCK_ID","NEXT_BLOCK_ID","GENERATOR_ID")
    for(i in id.cols) b[,i] = nxt.convert.id(b[,i])
  }
  
  return(b)
}
