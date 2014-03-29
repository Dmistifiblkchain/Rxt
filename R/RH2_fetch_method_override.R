library(RH2)

setMethod("fetch", signature(res="H2Result", n="numeric"), def=function(res, n, ...) {
  cols <- .jcall(res@md, "I", "getColumnCount")
  if (cols < 1) return(NULL)
  l <- list()
  for (i in 1:cols) {
    ct <- .jcall(res@md, "I", "getColumnType", i)
    l[[i]] <- if (ct == -5 | ct ==-6 | (ct >= 2 & ct <= 8)) { 
      numeric()
    } else if (ct == 91) { 
      structure(numeric(), class = "Date")
    } else if (ct == 92) { 
      structure(numeric(), class = "times")
    } else if (ct == 93) { 
      structure(numeric(), class = class(Sys.time()))
    } else character()
    names(l)[i] <- .jcall(res@md, "S", "getColumnName", i)
  }
  
  j <- 0
  while (.jcall(res@jr, "Z", "next")) {
    j <- j + 1
    for (i in 1:cols) {
      if (inherits(l[[i]], "Date")) {
        # browser() ##
        val <- .jcall(res@jr, "S", "getString", i)
        wn <- .jcall(res@jr, "Z", "wasNull")
        l[[i]][j] <- if (wn | is.null(val)) NA else as.Date(val)
      } else if (inherits(l[[i]], "times")) {
        val <- .jcall(res@jr, "S", "getString", i)
        wn <- .jcall(res@jr, "Z", "wasNull")
        l[[i]][j] <- if (wn | is.null(val)) NA else times(val)
      } else if (inherits(l[[i]], "POSIXct")) {
        val <- .jcall(res@jr, "S", "getString", i)
        wn <- .jcall(res@jr, "Z", "wasNull")
        l[[i]][j] <- if (wn | is.null(val)) NA else as.POSIXct(val)
      } else if (is.numeric(l[[i]])) { 
        val <- .jcall(res@jr, "D", "getDouble", i)
        wn <- .jcall(res@jr, "Z", "wasNull")
        l[[i]][j] <- if (wn | is.null(val)) NA else val
      } else {
        val <- .jcall(res@jr, "S", "getString", i)
        wn <- .jcall(res@jr, "Z", "wasNull")
        l[[i]][j] <- if (wn | is.null(val)) NA else val
      }
    }
    if (n > 0 && j >= n) break
  }
  if (j)
    as.data.frame(l, row.names=1:j)
  else
    as.data.frame(l)
})
