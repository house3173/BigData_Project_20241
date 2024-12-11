package com.bigdata.it4931.layer.application.service.batch;

import com.bigdata.it4931.layer.infrastructure.hdfs.IHdfsAdapter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@Service
@Slf4j
public class HdfsParquetService {
    private static final Schema SCHEMA;
    private static final String SCHEMA_LOCATION = "config/avroToParquet.avsc";

    static {
        try (InputStream inputStream = new FileInputStream(SCHEMA_LOCATION)) {
            SCHEMA = new Schema.Parser().parse(inputStream);
        } catch (Exception e) {
            log.error("Failed to load schema from " + SCHEMA_LOCATION, e);
            throw new RuntimeException(e);
        }
    }

    private final IHdfsAdapter hdfsAdapter;
    private final String datetimePattern = "yyyy-MM-dd-HH-mm-ss";
    private volatile ParquetWriter<GenericData.Record> writer;
    private final double fileSizeThreshold;
    private Path path;

    public HdfsParquetService(IHdfsAdapter hdfsAdapter){
        this.hdfsAdapter = hdfsAdapter;
        this.fileSizeThreshold = 127 * 1024 * 1024L;

        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    private void stop(){
        close();
    }

    public boolean close(){
        boolean existData = false;
        boolean success = false;
        try {
            if (writer != null){
                existData = writer.getDataSize() > 0;
                writer.close();
                log.info("Parquet writer closed");
                success = true;
            }
        } catch (Exception e){
            log.error("Failed to close parquet writer", e);
        } finally {
            if (existData && !success){
                log.error("Parquet writer closed with data loss");
            }
        }
        return success;
    }

    private void renameFile(Path src, Path dest){
        try {
            FileSystem fs = hdfsAdapter.getFileSystem();
            if (fs.exists(src)){
                fs.rename(src, dest);
                log.info("File {} renamed to {}", src, dest);
            }
        } catch (IOException e){
            log.error("Failed to rename file {} to {}", src, dest, e);
        }
    }

    private Path completeFile(Path path){
        Path completedPath = new Path(path.getParent() + "/" + path.getName().replace("pre.tmp", "tmp"));
        renameFile(path, completedPath);
        return completedPath;
    }

    private boolean writeToParquet(List<GenericData.Record> records){
        for (GenericData.Record record: records){
            try {
                writer.write(record);
            } catch (IOException e){
                log.error("Failed to write record to parquet", e);
                return false;
            }
        }
        return close();
    }

    private void initWriter(){
        String folder = hdfsAdapter.getNameNode() + "/bigdata";
        Optional<Path> optionalPath = Optional.empty();
        try {
            Path path = new Path(folder);
            FileSystem fs = hdfsAdapter.getFileSystem();
            if (!fs.exists(path)){
                fs.mkdirs(path);
            }
            optionalPath = Optional.of(new Path(path + "/pre.tmp." +
                    new SimpleDateFormat(datetimePattern).format(new Date()) + ".parquet"));
        } catch (IOException e){
            log.error("Failed to create parquet file", e);
        }

        if (optionalPath.isEmpty()) {
            log.error("Failed to create parquet file");
            return;
        }

        this.path = optionalPath.get();

        try {
            OutputFile outputFile = HadoopOutputFile.fromPath(path, hdfsAdapter.getConfiguration());
            this.writer = AvroParquetWriter.<GenericData.Record>builder(outputFile)
                    .withSchema(SCHEMA)
                    .withConf(hdfsAdapter.getConfiguration())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withWriteMode(ParquetFileWriter.Mode.CREATE)
                    .build();
        } catch (IOException e) {
            log.error("Failed to create parquet writer", e);
        }

    }
}

