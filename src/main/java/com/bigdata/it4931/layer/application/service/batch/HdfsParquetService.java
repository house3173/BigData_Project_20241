package com.bigdata.it4931.layer.application.service.batch;

import com.bigdata.it4931.layer.application.domain.dto.CompanyProfileDto;
import com.bigdata.it4931.layer.application.domain.dto.JobDataDto;
import com.bigdata.it4931.layer.infrastructure.hdfs.IHdfsAdapter;
import com.bigdata.it4931.utility.StringUtils;
import com.bigdata.it4931.utility.concurrent.AsyncCallback;
import com.bigdata.it4931.utility.concurrent.BatchProcessor;
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@Slf4j
public class HdfsParquetService {
    private static final Schema SCHEMA;
    private static final String SCHEMA_LOCATION = "config/avroToParquet.avsc";
    private final BatchProcessor<JobDataDto, Object> queue;
    private final ExecutorService taskExecutor;

    static {
        try (InputStream inputStream = new FileInputStream(SCHEMA_LOCATION)) {
            SCHEMA = new Schema.Parser().parse(inputStream);
        } catch (Exception e) {
            log.error("Failed to load schema from " + SCHEMA_LOCATION, e);
            throw new RuntimeException(e);
        }
    }

    private final IHdfsAdapter hdfsAdapter;
    private final String datetimePattern = "yyyy-MM-dd-HHmmss";
    private volatile ParquetWriter<GenericData.Record> writer;
    private final double fileSizeThreshold;
    private Path path;
    private volatile AtomicBoolean stopped = new AtomicBoolean(false);

    public HdfsParquetService(IHdfsAdapter hdfsAdapter,
                              @Qualifier("cacheExecutorService") ExecutorService taskExecutor) {
        this.hdfsAdapter = hdfsAdapter;
        this.fileSizeThreshold = 5 * 1024 * 1024L;
        this.taskExecutor = taskExecutor;

        queue = BatchProcessor.<JobDataDto, Object>newBuilder()
                .with(builder -> {
                    builder.executorService = taskExecutor;
                    builder.batchSize = 100;
                    builder.corePoolSize = 1;
                })
                .build(entries -> {
                    log.info("Processing batch of {} records", entries.size());

                    if (!stopped.get()){
                        checkFileSize();
                    }

                    List<JobDataDto> jobDataList = new ArrayList<>();
                    for (Map.Entry<JobDataDto, AsyncCallback<Object>> entry : entries) {
                        jobDataList.add(entry.getKey());
                    }
                    List<GenericData.Record> data = convertToParquetRecord(jobDataList);

                    log.info("Writing {} records to parquet", data.size());
                    writeToParquet(data);
                });


        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    private void stop() {
        try {
            this.stopped.set(true);
            queue.close();
            if (this.writer != null){
                this.writer.close();
            }
            taskExecutor.shutdown();
        } catch (IOException e) {
            log.error("Failed to close parquet writer", e);
        }
    }

    private synchronized void checkFileSize(){
        if (this.writer == null){
            log.info("Initializing parquet writer");
            initWriter();
            return;
        }
        if(this.writer.getDataSize() > fileSizeThreshold){
            log.info("File size threshold reached, closing current file");
            writeToNextFile();
        }
    }

    private void close(ParquetWriter<GenericData.Record> oldWriter, Path path) {
        boolean hasEmptyFile = false;
        try {
            if (oldWriter != null) {
                hasEmptyFile = oldWriter.getDataSize() == 0;
                oldWriter.close();

                FileSystem fs = hdfsAdapter.getFileSystem();
                if (fs.exists(path)){
                    Path completedPath = completeFile(path);
                    fs.rename(path, completedPath);
                }
            }
        } catch (IOException e) {
            log.error("Failed to close parquet writer", e);
            hasEmptyFile = true;
        } finally {
            try {
                if (hasEmptyFile) {
                    FileSystem fs = hdfsAdapter.getFileSystem();
                    if (fs.exists(path)) {
                        log.info("Deleting empty file {}", path);
                        fs.delete(path, true);
                    }
                }
            } catch (IOException e) {
                log.error("Failed to delete empty file", e);
            }
        }
    }

    private List<GenericData.Record> convertToParquetRecord(List<JobDataDto> jobDataList) {
        List<GenericData.Record> records = new ArrayList<>();
        jobDataList.forEach(jobData -> {
            GenericData.Record record = new GenericData.Record(SCHEMA);

            if (!StringUtils.isNullOrEmpty(jobData.getJobId())) {
                record.put("jobId", jobData.getJobId());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getExperience())) {
                record.put("experience", jobData.getExperience());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getQualifications())) {
                record.put("qualifications", jobData.getQualifications());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getSalaryRange())) {
                record.put("salaryRange", jobData.getSalaryRange());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getLocation())) {
                record.put("location", jobData.getLocation());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getCountry())) {
                record.put("country", jobData.getCountry());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getLatitude())) {
                record.put("latitude", jobData.getLatitude());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getLongitude())) {
                record.put("longitude", jobData.getLongitude());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getWorkType())) {
                record.put("workType", jobData.getWorkType());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getCompanySize())) {
                record.put("companySize", jobData.getCompanySize());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getJobPostingDate())) {
                record.put("jobPostingDate", jobData.getJobPostingDate());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getPreference())) {
                record.put("preference", jobData.getPreference());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getContactPerson())) {
                record.put("contactPerson", jobData.getContactPerson());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getContact())) {
                record.put("contact", jobData.getContact());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getJobTitle())) {
                record.put("jobTitle", jobData.getJobTitle());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getRole())) {
                record.put("role", jobData.getRole());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getJobPortal())) {
                record.put("jobPortal", jobData.getJobPortal());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getJobDescription())) {
                record.put("jobDescription", jobData.getJobDescription());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getBenefits())) {
                record.put("benefits", jobData.getBenefits());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getSkills())) {
                record.put("skills", jobData.getSkills());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getResponsibilities())) {
                record.put("responsibilities", jobData.getResponsibilities());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getCompanyName())) {
                record.put("companyName", jobData.getCompanyName());
            }

            GenericData.Record companyProfileRecord = new GenericData.Record(SCHEMA.getField("companyProfile").schema());
            CompanyProfileDto companyProfileDto = jobData.getCompanyProfile();
            if (!StringUtils.isNullOrEmpty(companyProfileDto.getSector())) {
                companyProfileRecord.put("sector", companyProfileDto.getSector());
            }
            if (!StringUtils.isNullOrEmpty(companyProfileDto.getIndustry())) {
                companyProfileRecord.put("industry", companyProfileDto.getIndustry());
            }
            if (!StringUtils.isNullOrEmpty(companyProfileDto.getCity())) {
                companyProfileRecord.put("city", companyProfileDto.getCity());
            }
            if (!StringUtils.isNullOrEmpty(companyProfileDto.getState())) {
                companyProfileRecord.put("state", companyProfileDto.getState());
            }
            if (!StringUtils.isNullOrEmpty(companyProfileDto.getZip())) {
                companyProfileRecord.put("zip", companyProfileDto.getZip());
            }
            if (!StringUtils.isNullOrEmpty(companyProfileDto.getWebsite())) {
                companyProfileRecord.put("website", companyProfileDto.getWebsite());
            }
            if (!StringUtils.isNullOrEmpty(companyProfileDto.getTicker())) {
                companyProfileRecord.put("ticker", companyProfileDto.getTicker());
            }
            if (!StringUtils.isNullOrEmpty(companyProfileDto.getCeo())) {
                companyProfileRecord.put("ceo", companyProfileDto.getCeo());
            }

            record.put("companyProfile", companyProfileRecord);

            records.add(record);
        });

        return records;
    }

    private void renameFile(Path src, Path dest) {
        try {
            FileSystem fs = hdfsAdapter.getFileSystem();
            if (fs.exists(src)) {
                fs.rename(src, dest);
                log.info("File {} renamed to {}", src, dest);
            }
        } catch (IOException e) {
            log.error("Failed to rename file {} to {}", src, dest, e);
        }
    }

    private Path completeFile(Path path) {
        Path completedPath = new Path(path.getParent() + "/" + path.getName().replace("pre.tmp", "tmp"));
        renameFile(path, completedPath);
        return completedPath;
    }

    private void writeToParquet(List<GenericData.Record> records) {
        for (GenericData.Record record : records) {
            try {
                writer.write(record);
            } catch (IOException e) {
                log.error("Failed to write record to parquet", e);
            }
        }
    }

    public synchronized void save(List<JobDataDto> jobDataList) {
        jobDataList.forEach(queue::put);
    }

    private void initWriter() {
        String folder = hdfsAdapter.getNameNode() + "/bigdata";
        Optional<Path> optionalPath = Optional.empty();
        try {
            Path path = new Path(folder);
            FileSystem fs = hdfsAdapter.getFileSystem();
            if (!fs.exists(path)) {
                fs.mkdirs(path);
            }
            optionalPath = Optional.of(new Path(path + "/pre.tmp." +
                    new SimpleDateFormat(datetimePattern).format(new Date()) + ".parquet"));
        } catch (IOException e) {
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

    private void writeToNextFile(){
        ParquetWriter<GenericData.Record> oldWriter = this.writer;
        Path oldPath = new Path(this.path.toString());
        initWriter();
        taskExecutor.submit(() -> close(oldWriter, oldPath));
    }
}

