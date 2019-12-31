package com.bigdata.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "spark")
public class SparkAndEsBean {

    private String appName;
    private String master;
    private String nodes;
    private String port;

    @Bean
    @ConditionalOnMissingBean(SparkConf.class)
    public SparkConf conf() {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster(master)
                .set("es.nodes", nodes)
                .set("es.port", port)
                .set("es.nodes.wan.only", "true")
                .set("spark.KryoSerializer", "config.MyKryoRegistrator");
        return conf;
    }

    @Bean
    @ConditionalOnMissingBean(JavaSparkContext.class)
    public JavaSparkContext sc() {
        return new JavaSparkContext(conf());
    }

    @Bean
    @ConditionalOnMissingBean(SparkSession.class)
    public SparkSession sparkSession() {
        SparkSession sparkSession = SparkSession.builder().config(conf()).getOrCreate();
        return sparkSession;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getNodes() {
        return nodes;
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }
}
