Bigdata-final
=============

Entity Linking with co-ranking

### Build
	$ mvn package

### Run
	$ hadoop jar target/entlink-0.1-job.jar INPUT OUTPUT

### Run with hadoop streaming

	$ hadoop jar HADOOP_STREAMING_JAR -D xmlinput.start='<page>' -D xmlinput.end='</page>' -libjars XmlInputFormat.jar -inputformat XmlInputFormat.XmlInputFormat -input INPUT -output OUTPUT -mapper MAPPER -file MAPPER -reducer REDUCER -file REDUCER
