package com.bigdata.controller;

import com.bigdata.service.SparkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SparkController {

    @Autowired
    SparkService sparkService;

    @RequestMapping("/spark")
    public Long reade() {
        String path = "file:\\D:\\记录与备份\\BigData\\wordcount\\wc.txt";
        return sparkService.readFile(path);
    }

    @RequestMapping("/sparkSQL")
    public void readES() {
        sparkService.readEs("cluster_details/cluster_type");
    }

    @RequestMapping("/hbCount/{tbName}")
    public void htbCount(@PathVariable String tbName) {
        sparkService.readHBaseTb(tbName);
    }

    @RequestMapping("/create/{HtableName}")
    boolean createHtb(@PathVariable String HtableName) {
        return sparkService.createTB(HtableName);
    }
}
