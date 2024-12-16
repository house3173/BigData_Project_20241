package com.bigdata.it4931.layer.application.service.batch;

import com.bigdata.it4931.config.hdfs.HdfsConfiguration;
import com.bigdata.it4931.layer.application.domain.dto.CompanyProfileDto;
import com.bigdata.it4931.layer.application.domain.dto.JobDataDto;
import com.bigdata.it4931.layer.infrastructure.hdfs.IHdfsAdapter;
import com.bigdata.it4931.utility.StringUtils;
import com.bigdata.it4931.utility.concurrent.AsyncCallback;
import com.bigdata.it4931.utility.concurrent.BatchProcessor;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@Slf4j
public class HdfsParquetService {
    private static final Schema SCHEMA;
    private static final String SCHEMA_LOCATION = "config/avroToParquet.avsc";
    private final BatchProcessor<JobDataDto, Object> queue;
    private final ExecutorService taskExecutor;
    private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    private final HdfsConfiguration hdfsConfiguration;

    static {
        try (InputStream inputStream = new FileInputStream(SCHEMA_LOCATION)) {
            SCHEMA = new Schema.Parser().parse(inputStream);
        } catch (Exception e) {
            log.error("Failed to load schema from " + SCHEMA_LOCATION, e);
            throw new RuntimeException(e);
        }
    }

    private final IHdfsAdapter hdfsAdapter;
    private volatile ParquetWriter<GenericData.Record> writer;
    private final long fileSizeThreshold;
    private Path path;
    private volatile AtomicBoolean stopped = new AtomicBoolean(false);

    public HdfsParquetService(IHdfsAdapter hdfsAdapter,
                              HdfsConfiguration hdfsConfiguration) {
        this.hdfsAdapter = hdfsAdapter;
        this.fileSizeThreshold = hdfsConfiguration.getHdfsFileMaxSize() * 1024 * 1024L - 502400;

        this.taskExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("cache-thread-pool-%d").build());
        this.hdfsConfiguration = hdfsConfiguration;

        queue = BatchProcessor.<JobDataDto, Object>newBuilder()
                .with(builder -> {
                    builder.executorService = taskExecutor;
                    builder.batchSize = 100;
                    builder.corePoolSize = 1;
                })
                .build(entries -> {
                    if (!stopped.get()){
                        try {
                            checkFileSize();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    List<JobDataDto> jobDataList = new ArrayList<>();
                    for (Map.Entry<JobDataDto, AsyncCallback<Object>> entry : entries) {
                        jobDataList.add(entry.getKey());
                    }
                    List<GenericData.Record> data = convertToParquetRecord(jobDataList);
                    try {
                        writeToParquet(data);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
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

    private void checkFileSize() throws IOException {
        synchronized (this) {
            if (writer == null) {
                initWriter();
                return;
            }
            long dataSize = this.writer.getDataSize();
            if (dataSize > fileSizeThreshold) {
                try {
                    writeToNextFile();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    private void close(ParquetWriter<GenericData.Record> writerClose, Path path) {
        boolean isSave = true;
        try {
            if (writerClose != null) {
                isSave = writerClose.getDataSize() > 1024 || (writerClose.getDataSize() > 0);
                writerClose.close();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            isSave = false;
        } finally {
            try {
                FileSystem fileSystem = FileSystem.get(hdfsAdapter.getConfiguration());
                if (!isSave && fileSystem.exists(path)) {
                    log.info("onDestroy: delete file empty : " + path);
                    fileSystem.delete(path, true);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    @Scheduled(cron = "0 0 1 * * *", zone = "Asia/Ho_Chi_Minh")
    public void eventFinalTempFile() {
        try {
            if (this.writer.getDataSize() > 0) {
                writeToNextFile();
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    private List<GenericData.Record> convertToParquetRecord(List<JobDataDto> jobDataList) {
        List<GenericData.Record> records = new ArrayList<>();
        jobDataList.forEach(jobData -> {
            GenericData.Record jobDataRecord = new GenericData.Record(SCHEMA);

            if (!StringUtils.isNullOrEmpty(jobData.getJobId())) {
                jobDataRecord.put("jobId", jobData.getJobId());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getExperience())) {
                jobDataRecord.put("experience", jobData.getExperience());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getQualifications())) {
                jobDataRecord.put("qualifications", jobData.getQualifications());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getSalaryRange())) {
                jobDataRecord.put("salaryRange", jobData.getSalaryRange());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getLocation())) {
                jobDataRecord.put("location", jobData.getLocation());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getCountry())) {
                jobDataRecord.put("country", jobData.getCountry());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getLatitude())) {
                jobDataRecord.put("latitude", jobData.getLatitude());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getLongitude())) {
                jobDataRecord.put("longitude", jobData.getLongitude());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getWorkType())) {
                jobDataRecord.put("workType", jobData.getWorkType());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getCompanySize())) {
                jobDataRecord.put("companySize", jobData.getCompanySize());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getJobPostingDate())) {
                jobDataRecord.put("jobPostingDate", jobData.getJobPostingDate());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getPreference())) {
                jobDataRecord.put("preference", jobData.getPreference());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getContactPerson())) {
                jobDataRecord.put("contactPerson", jobData.getContactPerson());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getContact())) {
                jobDataRecord.put("contact", jobData.getContact());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getJobTitle())) {
                jobDataRecord.put("jobTitle", jobData.getJobTitle());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getRole())) {
                jobDataRecord.put("role", jobData.getRole());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getJobPortal())) {
                jobDataRecord.put("jobPortal", jobData.getJobPortal());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getJobDescription())) {
                jobDataRecord.put("jobDescription", jobData.getJobDescription());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getBenefits())) {
                jobDataRecord.put("benefits", jobData.getBenefits());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getSkills())) {
                jobDataRecord.put("skills", jobData.getSkills());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getResponsibilities())) {
                jobDataRecord.put("responsibilities", jobData.getResponsibilities());
            }

            if (!StringUtils.isNullOrEmpty(jobData.getCompanyName())) {
                jobDataRecord.put("companyName", jobData.getCompanyName());
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

            jobDataRecord.put("companyProfile", companyProfileRecord);

            records.add(jobDataRecord);
        });

        return records;
    }

    private void writeToParquet(List<GenericData.Record> records) throws IOException {
        for (GenericData.Record jobDataRecord : records) {
            try {
                this.writer.write(jobDataRecord);
            } catch (IOException e) {
                log.error("Failed to write record to parquet", e);
            }
        }
    }

    public synchronized void save(List<JobDataDto> jobDataList) {
        jobDataList.forEach(queue::put);
    }

    private void initWriter() throws IOException {
        Optional<Path> optionalPath = this.getPath();
        if (optionalPath.isEmpty()){
            return;
        }

        this.path = optionalPath.get();
        OutputFile outputFile = HadoopOutputFile.fromPath(path, hdfsAdapter.getConfiguration());
        this.writer = AvroParquetWriter
                .<GenericData.Record>builder(outputFile)
                .withSchema(SCHEMA)
                .withConf(hdfsAdapter.getConfiguration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .build();

    }

    private Optional<Path> getPath(){
        String folderUrl = getFolder(formatter.format(new Date(System.currentTimeMillis())));
        try {
            Path folderPath = new Path(folderUrl);
            FileSystem fs = hdfsAdapter.getFileSystem();
            if (!fs.exists(folderPath)) {
                fs.mkdirs(folderPath);
            }
            return Optional.of(getNewFile(folderPath));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return Optional.empty();
    }

    private Path getNewFile(Path folderPath){
        String fileName = folderPath.toString() + '/' + new Timestamp(System.currentTimeMillis()).getTime() + ".parquet";
        return new Path(fileName);
    }

    private String getFolder(String day){
        return hdfsAdapter.getNameNode() + "/" + hdfsConfiguration.getHdfsFolder() + "/" + day;
    }

    private void writeToNextFile() throws IOException {
        log.info("Writing to next file");
        ParquetWriter<GenericData.Record> oldWriter = this.writer;
        Path oldPath = new Path(this.path.toString());
        initWriter();
        taskExecutor.submit(() -> close(oldWriter, oldPath));
    }
}

