package com.bigdata.config;

import com.bigdata.utils.HBaseUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class HBaseConfig {

    @Bean
    @ConditionalOnMissingBean(org.apache.hadoop.conf.Configuration.class)
    public org.apache.hadoop.conf.Configuration hconf() {
        org.apache.hadoop.conf.Configuration hconf = HBaseConfiguration.create();
        return hconf;
    }

    @Bean
    @ConditionalOnMissingBean(Connection.class)
    public Connection conn() {
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(hconf());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }

    @Bean
    public HBaseUtils hbaseUtils(){
        return new HBaseUtils();
    }
}
