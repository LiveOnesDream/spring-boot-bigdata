package com.bigdata;

import com.bigdata.service.SparkService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class DemoApplicationTests {

    @Autowired
    SparkService sparkService;

    @Test
    void test() {
        // String path = "file:\\D:\\记录与备份\\BigData\\wordcount\\wc.txt";
        // sparkService.readFile(path);
        // sparkService.readEs("cluster_details/cluster_type");
        boolean isCreate = sparkService.createTB("asa");
        System.out.println("表创建:" + isCreate);
    }
}
