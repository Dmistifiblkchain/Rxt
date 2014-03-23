nxt.genesis.ts <- as.POSIXct("2013-11-24 12:00:00",tz="UTC")

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

  # Protect against fatal crash referencing empty bigz object
  if (length(id)==0) return(id)
  
  if (from.db) {
    I = !is.na(id) & id<0 # Careful with NA that occurs when BLOCK_ID is empty
    id[I]=id[I] + 2*n
  } else {
    I = !is.na(id) & id>n
    id[I] = id[I] - 2*n
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
  
  # At least convert IDs to BIGZ
  require(gmp)
  bigint.cols = c("ID","PREVIOUS_BLOCK_ID","NEXT_BLOCK_ID","BASE_TARGET","GENERATOR_ID")
  for(i in bigint.cols) b[,i] = as.bigz(b[,i])
  
  if (ts.from.db) {
    b$TIMESTAMP = nxt.convert.ts(b$TIMESTAMP,from.db=TRUE)
  }

  if (id.from.db) {
    id.cols = c("ID","PREVIOUS_BLOCK_ID","NEXT_BLOCK_ID","GENERATOR_ID")
    for(i in id.cols) b[,i] = nxt.convert.id(b[,i],from.db=TRUE)
  }

  row.names(b)=as.character(b$ID)
  
  return(b)
}

nxt.getLastBlock <- function(con,nonzero.fee=FALSE) {
  q="SELECT max(HEIGHT) FROM PUBLIC.BLOCK"
  if (nonzero.fee)
    paste(q,"WHERE TOTAL_FEE>0")
  
  h=dbGetQuery(con,q)
  return(nxt.getBlocks(con,start.height=h,end.height=h))
}

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
  
  # At least convert IDs to BIGZ
  require(gmp)
  bigint.cols = c("ID","RECIPIENT_ID","SENDER_ID","REFERENCED_TRANSACTION_ID","BLOCK_ID")
  for(i in bigint.cols) b[,i] = as.bigz(b[,i])
  
  if (ts.from.db) {
    b$TIMESTAMP = nxt.convert.ts(b$TIMESTAMP,from.db=TRUE)
  }
  
  if (id.from.db) {
    id.cols = c("ID","RECIPIENT_ID","SENDER_ID","REFERENCED_TRANSACTION_ID","BLOCK_ID")
    for(i in id.cols) b[,i] = nxt.convert.id(b[,i],from.db=TRUE)
  }
  
  row.names(b)=as.character(b$ID)
  
  return(b)
}

nxt.getBalances <- function(con,account.ids=NULL,end.ts=NULL,id.from.db=TRUE) {
  w="WHERE TRUE"
  
  if (!is.null(end.ts)) {
    end.ts=nxt.convert.ts(end.ts,from.db=FALSE)
    w=paste(w," AND TIMESTAMP<=",end.ts,sep="")
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
    row.names(b)=as.character(b$ACCOUNT_ID)  
    return(b)
  }
  
  account.ids=nxt.convert.id(account.ids,from.db=id.from.db)
  cc=data.frame(ACCOUNT_ID=as.character(account.ids),BALANCE=0,
                stringsAsFactors=FALSE)
  row.names(cc)=as.character(account.ids)
  cc[as.character(b$ACCOUNT_ID),"BALANCE"]=b$BALANCE
  cc$ACCOUNT_ID = as.bigz(cc$ACCOUNT_ID)
  
  return(cc)
}

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

  # At least convert IDs to BIGZ
  require(gmp)
  bigint.cols = c("ACCOUNT_ID","TRANSACTION_ID","OTHER_ACCOUNT_ID")
  for(i in bigint.cols) b[,i] = as.bigz(b[,i])
  
  if (ts.from.db) {
    b$TIMESTAMP = nxt.convert.ts(b$TIMESTAMP,from.db=TRUE)
  }
  
  if (id.from.db) {
    id.cols = c("ACCOUNT_ID","TRANSACTION_ID","OTHER_ACCOUNT_ID")
    for(i in id.cols) b[,i] = nxt.convert.id(b[,i],from.db=TRUE)
  }
  
  if (calc.balance)
    b$BALANCE=cumsum(b$AMOUNT+b$FEE)
  
  return(b)
}

nxt.getAccountsStats <- function(con,account.ids=NULL,start.ts=NULL,end.ts=NULL,
                                 ts.from.db=TRUE,id.from.db=TRUE,calc.balance=TRUE) {

  w = rep("WHERE TRUE",3)
  
  if (!is.null(account.ids)) {
    i=c("SENDER_ID","RECIPIENT_ID","GENERATOR_ID")
    a=paste(nxt.convert.id(account.ids,from.db=FALSE),collapse=",")
    w=paste(w,"AND",i,"IN (",a,")")
  }
  
  if (!is.null(start.ts)) {
    start.ts=nxt.convert.ts(start.ts,from.db=FALSE)
    w=paste(w," AND TIMESTAMP>=",start.ts,sep="")
  }
  
  if (!is.null(end.ts)) {
    end.ts=nxt.convert.ts(end.ts,from.db=FALSE)
    w=paste(w," AND TIMESTAMP<=",end.ts,sep="")
  }
  
  q=paste("
SELECT CAST(r.ACCOUNT_ID AS VARCHAR) AS ACCOUNT_ID,
       N_REC, N_MESS_REC, N_MESS_SENDERS, NXT_REC, FIRST_REC, LAST_REC, N_TRANS, 
       FEE_PAID, NXT_SENT,  
       COALESCE(FIRST_SEND,-99999) AS FIRST_SEND, COALESCE(LAST_SEND,-99999) AS LAST_SEND,
       N_SEND, N_MESS_SENT, N_MESS_RECIPIENTS, N_ALIAS_ASSIGNS, N_COLORED_TRANS,
       N_FORGED, NONZERO_N_FORGED, FEE_FORGED, 
       COALESCE(FIRST_FORGED,-99999) AS FIRST_FORGED, 
       COALESCE(LAST_FORGED,-99999) AS LAST_FORGED,
       COALESCE(NONZERO_FIRST_FORGED,-99999) AS NONZERO_FIRST_FORGED, 
       COALESCE(NONZERO_LAST_FORGED,-99999) AS NONZERO_LAST_FORGED
FROM
(
SELECT RECIPIENT_ID AS ACCOUNT_ID,
       sum(TYPE=0) AS N_REC,
       sum(TYPE=1 AND SUBTYPE=0) AS N_MESS_REC,
       count(DISTINCT (CASE WHEN TYPE=1 AND SUBTYPE=0 THEN SENDER_ID ELSE NULL END)) AS N_MESS_SENDERS,
       sum(AMOUNT) AS NXT_REC,
       min(TIMESTAMP) AS FIRST_REC,
       max(TIMESTAMP) AS LAST_REC
FROM PUBLIC.TRANSACTION",w[2],"
GROUP BY RECIPIENT_ID
) AS r LEFT JOIN
(
SELECT SENDER_ID AS ACCOUNT_ID, 
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
FROM PUBLIC.TRANSACTION",w[1]," 
GROUP BY SENDER_ID
) AS s ON r.ACCOUNT_ID=s.ACCOUNT_ID LEFT JOIN
(
SELECT GENERATOR_ID AS ACCOUNT_ID,
       count(*) AS N_FORGED,
       sum(TOTAL_FEE>0) AS NONZERO_N_FORGED,
       sum(TOTAL_FEE) AS FEE_FORGED,
       min(TIMESTAMP) AS FIRST_FORGED,
       max(TIMESTAMP) AS LAST_FORGED,               
       min(CASE WHEN TOTAL_FEE>0 THEN TIMESTAMP ELSE NULL END) AS NONZERO_FIRST_FORGED,
       max(CASE WHEN TOTAL_FEE>0 THEN TIMESTAMP ELSE NULL END) AS NONZERO_LAST_FORGED
FROM PUBLIC.BLOCK",w[3]," 
GROUP BY GENERATOR_ID
) AS g ON r.ACCOUNT_ID=g.ACCOUNT_ID
ORDER BY r.ACCOUNT_ID
")

  b=dbGetQuery(con,q)
  
  # Replace -99999 with NaN
  b[b==-99999]=NaN
  
  # Change factors back into character strings - there must be a way to avoid conversion to factor
  I = sapply(b,class)=="factor"
  b[,I] = sapply(b[,I],as.character)
  
  # At least convert IDs to BIGZ
  require(gmp)
  b$ACCOUNT_ID = as.bigz(b$ACCOUNT_ID)

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
  
  if (id.from.db) {
    b$ACCOUNT_ID = nxt.convert.id(b$ACCOUNT_ID,from.db=TRUE)
  }
  row.names(b)=as.character(b$ACCOUNT_ID)
  
  if (calc.balance)
    b$BALANCE = b$NXT_REC - b$NXT_SENT - b$FEE_PAID + b$FEE_FORGED
  
  if (!is.null(account.ids) & dim(b)[1]!=length(account.ids))
    warning("Data for some account IDs not found")
  
  return(b)
}

#nxt.getAliasesOwned <- function(con,account.ids=NULL,end.ts=NULL,id.from.db=TRUE) {}
