\name{enableMosaic}
\alias{enableMosaic}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
}
\usage{
enableMosaic(geometryAPI = "ESRI", indexSystem = "H3")
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{geometryAPI}{
%%     ~~Describe \code{geometryAPI} here~~
}
  \item{indexSystem}{
%%     ~~Describe \code{indexSystem} here~~
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
%%  ~~who you are~~
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

## The function is currently defined as
function (geometryAPI = "ESRI", indexSystem = "H3") 
{
    geometry_api <- sparkR.callJStatic(x = "com.databricks.labs.mosaic.core.geometry.api.GeometryAPI", 
        methodName = "apply", geometryAPI)
    index_system_id <- sparkR.callJStatic(x = "com.databricks.labs.mosaic.core.index.IndexSystemID", 
        methodName = "apply", indexSystem)
    indexing_system <- sparkR.callJStatic(x = "com.databricks.labs.mosaic.core.index.IndexSystemID", 
        methodName = "getIndexSystem", index_system_id)
    mosaic_context <- sparkR.newJObject(x = "com.databricks.labs.mosaic.functions.MosaicContext", 
        indexing_system, geometry_api)
    functions <<- sparkR.callJMethod(mosaic_context, "functions")
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory (show via RShowDoc("KEYWORDS")):
% \keyword{ ~kwd1 }
% \keyword{ ~kwd2 }
% Use only one keyword per line.
% For non-standard keywords, use \concept instead of \keyword:
% \concept{ ~cpt1 }
% \concept{ ~cpt2 }
% Use only one concept per line.
