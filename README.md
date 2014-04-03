# Rxt - Functions for analyzing the H2 database containing the NXT blockchain

## Description

This package contains a series of R functions for accessing and
analyzing data in the NXT blockchain, which is stored in a H2 (java)
database.  To use this package, one must either connect to a copy of
the H2 database that is not in active use by the (NXT) NRS Client or
instruct the NRS client to open the database with the AUTO_SERVER=TRUE
option using the nxt.dbUrl property.

## Maintainer

[David M. Kaplan](mailto:dmkaplan2000@gmail.com)

## Installation of latest version via github

The latest development version of Rxt can be installed directly from
[github](https://github.com/) using the
[devtools](http://cran.r-project.org/web/packages/devtools/index.html)
R package. First that package needs to be installed:

     install.packages("devtools")

Once this is done, one can install the development version with:

     require("devtools")
     install_github("Rxt","dmkaplan2000","master")

Replace "master" with another branch, tag or commit to obtain a
different version of the package.

Rxt currently requires the package
[RH2](https://github.com/dmkaplan2000/RH2) with version >=0.2, which
is not yet on [CRAN](http://cran.r-project.org/). The latest version
of RH2 can also be installed from github following the instructions
[here](https://github.com/dmkaplan2000/RH2/blob/master/README.md).

