package org.example.sparkdemo.config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        SparkSession spark =  SparkSession.builder()
                .appName("spark-code")
                .master("spark://localhost:7077")
                .config("spark.driver.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
                .config("spark.executor.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
                .config("spark.jars", "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar")
                .getOrCreate();

        try {
            // test query đơn giản
            Dataset<Row> data = spark.sql("SELECT 1 AS id, 'test' AS value UNION ALL SELECT 2, 'test2'");
            data.show();

            // Nếu muốn đọc từ PostgreSQL
            Dataset<Row> pgData = spark.read()
                    .format("jdbc")
                    .option("url", "jdbc:postgresql://postgres:5432/spark_demo_db")
                    .option("dbtable", "your_table") // thay bằng tên bảng thật
                    .option("user", "admin")
                    .option("password", "password")
                    .option("driver", "org.postgresql.Driver")
                    .load();

            System.out.println("Data from PostgreSQL:");
            pgData.show();

        } catch (Exception e) {
            System.err.println("Error stack trace: " + e.getMessage());
            e.printStackTrace();
            throw new IllegalStateException("Spark job failed", e);
        } finally {
            if (spark != null) {
                spark.stop();
            }
        }
    }
}
