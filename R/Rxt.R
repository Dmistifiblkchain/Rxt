nxtDbConnect <- function(file="nxt/nxt_db/nxt.h2.db",H2.opts="DB_CLOSE_ON_EXIT=FALSE;AUTO_SERVER=TRUE",
                         H2.prefix="jdbc:h2",username="sa",password="sa") {
  
  
  con <- dbConnect(H2(),"jdbc:h2:~/cryptocurrencies/nxt-coin/nxt/nxt_db/nxt;DB_CLOSE_ON_EXIT=FALSE;AUTO_SERVER=TRUE","sa","sa")
  
}
