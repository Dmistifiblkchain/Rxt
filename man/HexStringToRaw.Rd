\name{HexStringToRaw}
\alias{HexStringToRaw}
\title{Converts hex strings to raw R objects}
\usage{
HexStringToRaw(s)
}
\arguments{
  \item{s}{Single hex string. Use \code{\link{apply}} or
  \code{\link{sapply}} to work with vectors of hex strings}
}
\value{
Raw R object corresponding to hex string
}
\description{
Converts hex strings (e.g., "ac6fef") to a
\code{\link{raw}} R object
}
\author{
David M. Kaplan \email{dmkaplan2000@gmail.com}
}
\seealso{
\code{\link{raw}}, \code{\link{rawToChar}},
\code{\link{RawToHexString}}
}

