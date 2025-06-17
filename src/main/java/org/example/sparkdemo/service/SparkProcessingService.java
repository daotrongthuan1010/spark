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

import java.util.List;
import java.util.concurrent.TimeoutException;

@Service
public class SparkProcessingService {

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
                .setMaster(sparkMaster)
                .set("spark.driver.host", "localhost")
                .set("spark.streaming.stopGracefullyOnShutdown", "true");

        // Khởi tạo Spark Session
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // Định nghĩa schema cho dữ liệu từ Kafka (giả sử dữ liệu là JSON)
        StructType schema = new StructType()
                .add("product", "string")
                .add("quantity", "integer")
                .add("price", "double")
                .add("timestamp", "timestamp");

        // Đọc dữ liệu từ Kafka bằng Spark Structured Streaming
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

        // Xử lý dữ liệu (tính tổng doanh thu theo sản phẩm)
        Dataset<Row> salesByProduct = salesData.groupBy("product")
                .agg(
                        org.apache.spark.sql.functions.sum(
                                org.apache.spark.sql.functions.col("quantity").multiply(
                                        org.apache.spark.sql.functions.col("price")
                                )
                        ).as("total_revenue")
                )
                .orderBy(org.apache.spark.sql.functions.col("total_revenue").desc());

        // Hiển thị kết quả trên console (streaming)
        salesByProduct.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        // Lưu kết quả vào thư mục output (batch hoặc streaming)
        salesByProduct.write()
                .format("csv")
                .option("header", "true")
                .save("/output/sales_by_product");

        // Chờ streaming kết thúc (nếu cần)
        spark.streams().awaitAnyTermination();

        // Đóng Spark Session
        spark.stop();
    }

    public void sendDataToKafka() {
        // Sử dụng paging hoặc batch để đọc dữ liệu từ PostgreSQL
        int pageSize = 1000; // Số bản ghi mỗi batch
        long totalRecords = salesDataRepository.count(); // Tổng số bản ghi
        for (long offset = 0; offset < totalRecords; offset += pageSize) {
            Page<SalesData> salesPage = salesDataRepository.findAll(PageRequest.of((int) (offset / pageSize), pageSize));
            for (SalesData sale : salesPage) {
                String jsonData = String.format(
                        "{\"product\":\"%s\",\"quantity\":%d,\"price\":%.2f,\"timestamp\":\"%s\"}",
                        sale.getProduct(), sale.getQuantity(), sale.getPrice(), sale.getTimestamp()
                );
                kafkaProducerService.sendSalesData("sales-topic", jsonData);
            }
        }
    }
}
