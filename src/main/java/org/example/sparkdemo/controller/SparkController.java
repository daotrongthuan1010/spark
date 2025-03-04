package org.example.sparkdemo.controller;

import org.apache.spark.sql.streaming.StreamingQueryException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.TimeoutException;

@RestController
public class SparkController {

    @Autowired
    private org.example.sparkdemo.service.SparkProcessingService sparkProcessingService;

    @GetMapping("/process-spark")
    public String processSparkData() throws StreamingQueryException, TimeoutException {
        // Gửi dữ liệu từ PostgreSQL sang Kafka
        sparkProcessingService.sendDataToKafka();
        // Xử lý dữ liệu với Spark từ Kafka
        sparkProcessingService.processSalesData();
        return "Spark processing with Kafka completed!";
    }
}