package com.anil;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.*;
//cassandra connector
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.driver.core.Session;

/**
 * Created by Anil Ramapanicker on 5/10/17.
 */
public class MakeReport implements Serializable {
    private transient SparkConf conf;

    private MakeReport(SparkConf conf){
        this.conf = conf;
    }

    private void run(){
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        createCassandraTable(sparkSession);
        readCSVFileAndWritToCassandra(sparkSession);
        readCassandraTableWriteAsParquet(sparkSession);
    }
    private void createCassandraTable(SparkSession spark){
        CassandraConnector connector = CassandraConnector.apply(conf);
        try(Session session = connector.openSession()){
            session.execute("DROP KEYSPACE IF EXISTS ubs");
            session.execute("CREATE KEYSPACE IF NOT EXISTS ubs WITH replication={'class':'SimpleStrategy','replication_factor':1}");
            session.execute("CREATE TABLE IF NOT EXISTS ubs.people(name TEXT PRIMARY KEY,age INT)");

        }

    }
    private void readCSVFileAndWritToCassandra(SparkSession spark){
        Dataset<Row> df = spark.read().option("header","true").csv("file:/root/IdeaProjects/CassandraTest/data/people.csv");// just hardcoded
        df.show();
        Map cassandraoptions = new HashMap<String,String>();
        cassandraoptions.put("table","people");
        cassandraoptions.put("keyspace","ubs");
        df.write().format("org.apache.spark.sql.cassandra")
                .options(cassandraoptions).save();

    }
    private void readCassandraTableWriteAsParquet(SparkSession spark){
        Map cassandraoptions = new HashMap<String,String>();
        cassandraoptions.put("table","people");
        cassandraoptions.put("keyspace","ubs");
        Dataset<Row> df = spark.read().format("org.apache.spark.sql.cassandra").options(cassandraoptions).load();
        df.createOrReplaceTempView("people");
        Dataset<Row> df2 = spark.sql("select * from people WHERE age<40");
        df2.show();
        df2.write().saveAsTable("people2");


    }

    public static void main(String[] args) {
        if(args.length != 2){
            System.err.println("Usage:\n com.anil.MakeReport <Spark Master> <Cassandra IP Address>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf();
        String appName = "MakeReport";
        String cassndraHost = "spark.cassandra.connection.host";
        conf.setAppName(appName);
        conf.setMaster(args[0]);
        conf.set(cassndraHost, args[1]);
        MakeReport reportApp = new MakeReport(conf);
        reportApp.run();


    }
}
