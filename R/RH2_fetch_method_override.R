library(RH2)

dmkfetchmethod<-function(res, n, ...) {
  # Column count
  cols <- .jcall(res@md, "I", "getColumnCount")
  
  # Row count  
  nc <- .jcall(res@jr, "I", "getRow")
  .jcall(res@jr, "Z", "absolute",as.integer(-1)) # Last row
  nl <- .jcall(res@jr, "I", "getRow")
  .jcall(res@jr, "Z", "absolute",as.integer(nc)) # Return to previous row
  nn <- nl - nc # Remaining rows
  if (n<0 | nn<n) n <- nn
  
  # Get column names
  cns <- sapply( 1:cols,function(i) .jcall(res@md, "S", "getColumnName", i) )
  
  # Get column types
  cts <- sapply( 1:cols,function(i) .jcall(res@md, "I", "getColumnType", i) )
  
  # Corresponding R data types
  rts <- rep("character",cols)
  I = which(cts == -5 | cts ==-6 | (cts >= 2 & cts <= 8))
  rts[I] <- "numeric"
  I = which(cts == 91)
  rts[I] <- "Date"
  I = which(cts == 92)
  rts[I] <- "times"
  I = which(cts == 93)
  rts[I] <- "POSIXct"
  
  jts <- c(numeric="getDouble",character="getString",Date="getString",
           times="getString",POSIXct="getString")[rts]
  jnis <- c(numeric="D",character="S",Date="S",times="S",POSIXct="S")[rts] 
  pps <- c(numeric=as.numeric,character=as.character,Date=as.Date,times=times,
           POSIXct=as.POSIXct)[rts]

  # Pre-create result list
  l <- list()
  for (i in 1:cols) l[[i]] = (pps[[i]])(rep(NA,n))
  
  names(l) <- cns

  if (n==0) return(as.data.frame(l,stringsAsFactors=FALSE))
  
  for (j in 1:n) {
    if (j %% 1000==0) print(paste("Retrieving row",j))
    if (!(.jcall(res@jr, "Z", "next")))
      stop("Row not found") # Should never happen
    
    for (i in 1:cols) {
      val <- .jcall(res@jr, jnis[[i]], jts[[i]], i)
      if (!(.jcall(res@jr, "Z", "wasNull"))) l[[i]][j] <- (pps[[i]])(val)
    }
  }

  # Just changes attributes to avoid large copy
  attr(l, 'class') <- 'data.frame'
  attr(l, "row.names") <- c(NA_integer_,n)
  return(l)
}

setMethod("fetch", signature(res="H2Result", n="numeric"), def=dmkfetchmethod)

setMethod("dbSendQuery", signature(conn="H2Connection", statement="character"),  def=function(conn, statement, ..., list=NULL) {
  s <- .jcall(conn@jc, "Ljava/sql/PreparedStatement;", "prepareStatement", as.character(statement)[1],
              as.integer(.jfield("java/sql/ResultSet","I","TYPE_SCROLL_INSENSITIVE")), 
              as.integer(.jfield("java/sql/ResultSet","I","CONCUR_READ_ONLY")), 
              check=FALSE)
  .verify.JDBC.result(s, "Unable to execute JDBC statement ",statement)
  if (length(list(...))) .fillStatementParameters(s, list(...))
  if (!is.null(list)) .fillStatementParameters(s, list)
  r <- .jcall(s, "Ljava/sql/ResultSet;", "executeQuery", check=FALSE)
  .verify.JDBC.result(r, "Unable to retrieve JDBC result set for ",statement)
  md <- .jcall(r, "Ljava/sql/ResultSetMetaData;", "getMetaData", check=FALSE)
  .verify.JDBC.result(md, "Unable to retrieve JDBC result set meta data for ",statement, " in dbSendQuery")
  new("H2Result", jr=r, md=md)
})
