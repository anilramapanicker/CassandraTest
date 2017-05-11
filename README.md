# CassandraTest
A Sample Cassandra Spark Application 
Reads a CSV file in to Spark Datatset 
Writes the dataset to Cassandra Table 
Reads the Cassandra Table to Dataset 
Filter out few Writes it as a Parquet File

// clone // modify the data directory path // modify the cassandra ip address in the runApp.sh

to compile: $ mvn package to run $ sh runApp.sh

We need the cassandra connector assembly at classpath
