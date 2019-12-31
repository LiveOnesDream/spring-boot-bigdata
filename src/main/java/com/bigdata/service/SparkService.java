package com.bigdata.service;

import com.bigdata.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;


import java.io.IOException;

@Service
public class SparkService {

    @Autowired
    transient JavaSparkContext sc;

    @Autowired
    transient SparkSession sparkSession;

    @Autowired
    Connection conn;

    @Autowired
    org.apache.hadoop.conf.Configuration hconf;

    @Autowired
    HBaseUtils hbaseUtils;

    public long readFile(String path) {
        JavaRDD<String> lineRDD = sc.textFile(path);
        long count = lineRDD.count();
        System.out.println(count);
        return count;
    }

    public void readEs(String indexName) {
        JavaEsSparkSQL.esDF(sparkSession, indexName).createOrReplaceTempView("index");
        sparkSession.sql("SELECT * FROM index order by id desc").show(1000);
    }

    public boolean createTB(String tbName) {
        return hbaseUtils.createTable(tbName);
    }

    public void readHBaseTb(String tbName) {
        Scan scan = new Scan();
        try {
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String scanToString = Base64.encodeBytes(proto.toByteArray());
            hconf.set(TableInputFormat.INPUT_TABLE, tbName);
            hconf.set(TableInputFormat.SCAN, scanToString);
            JavaPairRDD<ImmutableBytesWritable, Result> hbaserdd = sc.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            long count = hbaserdd.count();
            System.out.println(count);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
