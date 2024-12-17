package com.bigdata.it4931.layer.application.service.speed;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

@Service
@Slf4j
public class SpeedConsumer {
    private final SparkConf conf;
    private final SparkSession spark;

    public SpeedConsumer(){
        this.conf = new SparkConf()
                .setAppName("SpeedConsumer")
                .setMaster("spark://spark-singlenode:7077")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrationRequired", "false")
                .set("spark.executor.memory", "1g")
                .set("spark.executor.cores", "1")
                .set("spark.driver.memory", "1g");
        this.spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));

    }

    protected void stop() {
        log.info("Stopping Spark session...");
        spark.stop();
    }

    public void processStream() {
        log.info("Processing stream...");
        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-server:9092")
                .option("subscribe", "bigdata")
                .option("startingOffsets", "earliest")
                .load();

        df.printSchema();

        Dataset<Row> processedDF = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp");

        processedDF.printSchema();

        StructType companyProfileSchema = new StructType()
                .add("Sector", DataTypes.StringType)
                .add("Industry", DataTypes.StringType)
                .add("City", DataTypes.StringType)
                .add("State", DataTypes.StringType)
                .add("Zip", DataTypes.StringType)
                .add("Website", DataTypes.StringType)
                .add("Ticker", DataTypes.StringType)
                .add("CEO", DataTypes.StringType);

        StructType jsonSchema = new StructType()
                .add("jobId", DataTypes.StringType)
                .add("experience", DataTypes.StringType)
                .add("qualifications", DataTypes.StringType)
                .add("salaryRange", DataTypes.StringType)
                .add("location", DataTypes.StringType)
                .add("country", DataTypes.StringType)
                .add("latitude", DataTypes.StringType)
                .add("longitude", DataTypes.StringType)
                .add("workType", DataTypes.StringType)
                .add("companySize", DataTypes.StringType)
                .add("jobPostingDate", DataTypes.StringType)
                .add("preference", DataTypes.StringType)
                .add("contactPerson", DataTypes.StringType)
                .add("contact", DataTypes.StringType)
                .add("jobTitle", DataTypes.StringType)
                .add("role", DataTypes.StringType)
                .add("jobPortal", DataTypes.StringType)
                .add("jobDescription", DataTypes.StringType)
                .add("benefits", DataTypes.StringType)
                .add("skills", DataTypes.StringType)
                .add("responsibilities", DataTypes.StringType)
                .add("companyName", DataTypes.StringType)
                .add("companyProfile", companyProfileSchema); // Schema lồng ghép

        Dataset<Row> jsonDF = processedDF.selectExpr("CAST(value AS STRING) as value")
                .select(from_json(col("value"), jsonSchema).as("data"))
                .select("data.*");

        jsonDF.printSchema();

        try {
            jsonDF.writeStream()
                    .format("memory")
                    .queryName("job_data")
                    .outputMode("append")
                    .trigger(Trigger.ProcessingTime("10 seconds"))
                    .start();
        } catch (Exception e) {
            log.error("Error while processing stream", e);
        }
    }
}

