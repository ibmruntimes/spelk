# Spelk - reporting Apache Spark metrics to Elasticsearch

Spelk (spark-elk) is an add-on to Apache Spark (<http://spark.apache.org/>) to report metrics to an Elasticsearch server and allow visualizations using Kibana.

## Building Spelk

Spelk is built using [Apache Maven](http://maven.apache.org/). You can specify the Spark and Scala versions.

    mvn -Dspark.version=1.6.1 -Dscala.binary.version=2.10 clean package


## Configuration

To use Spelk you need to add the Spelk jar to the Spark driver/executor classpaths and enable the metrics sink.


**Add Spelk jar to classpaths**

Update [spark home]/conf/spark-defaults.conf eg.

	spark.driver.extraClassPath=<path to Spelk>/spelk-0.1.0.jar
	spark.executor.extraClassPath=<path to Spelk>/spelk-0.1.0.jar


 
**Enable the Elasticsearch sink**

Update metrics.properties in [spark home]/conf/metrics.properties eg.

	# org.apache.spark.elk.metrics.sink.ElasticsearchSink
	driver.sink.elk.class=org.apache.spark.elk.metrics.sink.ElasticsearchSink
	executor.sink.elk.class=org.apache.spark.elk.metrics.sink.ElasticsearchSink
	#   Name:     Default:      Description:
	#   host      none          Elasticsearch server host	
	#   port      none          Elasticsearch server port 
	#   index     spark         Elasticsearch index name
	#   period    10            polling period
	#   units     seconds       polling period units
	*.sink.elk.host=localhost
	*.sink.elk.port=9200
	*.sink.elk.index=spark
	*.sink.elk.period=10
	*.sink.elk.unit=seconds
.
