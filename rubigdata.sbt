name		:= "RUBigDataRecipes"
version		:= "1.0"
scalaVersion	:= "2.10.5"

packAutoSettings

val sparkV	= "1.6.1"
val hadoopV	= "2.7.1"
val jwatV	= "1.0.0"
val jsoupV = "1.9.2"
resolvers += "nl.surfsara" at "http://beehub.nl/surfsara-repo/releases" //for the jwat packages

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkV  % "provided" ,
  "org.apache.hadoop" %  "hadoop-client" % hadoopV  % "provided",
  "org.jwat"          % "jwat-common"    % jwatV,
  "org.jwat"          % "jwat-warc"      % jwatV,
  "org.jwat"          % "jwat-gzip"      % jwatV,
  "nl.surfsara" % "warcutils" % "1.3",
  "org.jsoup" % "jsoup" % jsoupV //html parsing

)
libraryDependencies += "me.gladwell.microtesia" %% "microtesia" % "0.3" //parse microformat
libraryDependencies += "com.google.code.gson" % "gson" % "2.7" //output json
