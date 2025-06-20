package org.example.sparkdemo.service;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.example.sparkdemo.entity.SalesData;
import org.example.sparkdemo.repository.SalesDataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

@Service
public class SparkProcessingService {

    @Autowired
    private ExecutorService executorService;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;

    @Autowired
    private SalesDataRepository salesDataRepository;

    @Value("${spark.master}")
    private String sparkMaster;

    public void processSalesData() throws StreamingQueryException, TimeoutException {
        // Cấu hình Spark để kết nối với Spark Master trong Docker
        SparkConf conf = new SparkConf()
                .setAppName("SparkDemo")
                .setMaster("local[*]")
                .set("spark.driver.host", "host.docker.internal") // Hoặc IP của host
                .set("spark.streaming.stopGracefullyOnShutdown", "true");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        StructType schema = new StructType()
                .add("product", "string")
                .add("quantity", "integer")
                .add("price", "double")
                .add("timestamp", "timestamp");

        Dataset<Row> salesData = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", "sales-topic")
                .option("failOnDataLoss", "false")
                .load()
                .selectExpr("CAST(value AS STRING) as json")
                .select(org.apache.spark.sql.functions.from_json(
                        org.apache.spark.sql.functions.col("json"), schema
                ).as("data"))
                .select("data.*");

        Dataset<Row> salesByProduct = salesData.groupBy("product")
                .agg(
                        org.apache.spark.sql.functions.sum(
                                org.apache.spark.sql.functions.col("quantity").multiply(
                                        org.apache.spark.sql.functions.col("price")
                                )
                        ).as("total_revenue")
                )
                .orderBy(org.apache.spark.sql.functions.col("total_revenue").desc());

        salesByProduct.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        salesByProduct.write()
                .format("csv")
                .option("header", "true")
                .save("/output/sales_by_product");

        spark.streams().awaitAnyTermination();

        spark.stop();
    }

     // Cấu hình thread pool
    public void sendDataToKafka() {
        int pageSize = 100000;
        long totalRecords = salesDataRepository.count();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (long offset = 0; offset < totalRecords; offset += pageSize) {
            int pageIndex = (int) (offset / pageSize);

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                Page<SalesData> salesPage = salesDataRepository.findAll(PageRequest.of(pageIndex, pageSize));
                for (SalesData sale : salesPage) {
                    String topic = getKafkaTopicByPrice(sale.getPrice());

                    String jsonData = String.format(
                            "{\"product\":\"%s\",\"quantity\":%d,\"price\":%.2f,\"timestamp\":\"%s\"}",
                            sale.getProduct(), sale.getQuantity(), sale.getPrice(), sale.getTimestamp()
                    );

                    kafkaProducerService.sendSalesData(topic, jsonData);
                }
            }, executorService);

            futures.add(future);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    // Hàm hỗ trợ phân loại topic theo mức giá
    private String getKafkaTopicByPrice(BigDecimal price) {
        if (price.compareTo(BigDecimal.valueOf(1_000_000)) < 0) {
            return "low-price-topic";
        } else if (price.compareTo(BigDecimal.valueOf(10_000_000)) < 0) {
            return "mid-price-topic";
        } else {
            return "high-price-topic";
        }
    }


}
