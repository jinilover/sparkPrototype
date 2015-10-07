#Spark examples
##Pre-requisite
* Don't need to install Spark
* Once got class not found error for scala classes during runtime where using scala 2.11.x.  The problem seems to be gone after setting to scala 2.11.7 and spark 1.4.1.

##Apache log analysis
It does different basic calculation on the apache log files, e.g. how many requests sent for different response codes etc.

##Use of Spark, Free Monad
It's mainly divided into:
* ```LogDataAnalysis.scala``` - contains the Free Monad dsl script and business logic, independent of any specific big data technology tools being used.
* ```AnalyzeLogInterpreter``` - act as the Free Monad interpreter, encapsulates the Spark api from ```LogDataAnalysis.scala```.  Uses those commonly used RDD/Pair RDD API such as ```filter```, ```map```, ```reduce```, etc.  All these functions are similar to the higher-order functions in FP.  That's why FP languages are good match for big data processing.

##Note about the RDD API
Some little note
* The transformation operations (such as ```map```, ```filter```) are lazy in nature and creates new RDDs.  Only action operations (such as ```count```, ```reduce```) can trigger the transformation such that the transformation will evaluate again.  For better performance, ```.cache``` is used to store the intermediate RDD in memory.  E.g. the ```parsedLogRecs``` RDD is cached once it is created.  ```parsedLogRecs``` is used in various places for different analysis, caching it will avoid ```parsedLogRecs``` from being re-created.
* RDD is a distributed collecton of data items across a number of "partition" which may be allocated to different compute nodes.  When we pass a function literal to a higher-order function, Spark passes it to different nodes to analyse the corresponding data items of that particular node.  The variables involved should be inside the scope of the function literal such that Spark can pass it to the node.  E.g. ```LOG_MSG_FORMAT_PATTERN``` inside function ```parseRawString```.

##How to execute
E.g. I download a file from ftp://ita.ee.lbl.gov/traces/calgary_access_log.gz, unzip it as ```usask_access_log``` under folder ```sparkPrototype```.  Under the Scala shell, type ```runMain loganalysis.Main calgary_access_log```.  It will take a while (< 1 min) to finish.  Different log files can be downloaded from http://ita.ee.lbl.gov/html/traces.html.