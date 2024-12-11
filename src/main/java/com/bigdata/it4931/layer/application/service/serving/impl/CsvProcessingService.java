package com.bigdata.it4931.layer.application.service.serving.impl;

import com.bigdata.it4931.config.Constants;
import com.bigdata.it4931.layer.application.domain.dto.CompanyProfileDto;
import com.bigdata.it4931.layer.application.domain.dto.JobDataDto;
import com.bigdata.it4931.layer.application.service.serving.ICsvProcessingService;
import com.bigdata.it4931.layer.infrastructure.hdfs.IHdfsAdapter;
import com.bigdata.it4931.layer.infrastructure.kafka.write.KafkaBrokerWriter;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

@Service
@Slf4j
public class CsvProcessingService implements ICsvProcessingService {
    private final KafkaBrokerWriter kafkaBrokerWriter;
    private final IHdfsAdapter hdfsAdapter;

    public CsvProcessingService(@Qualifier("kafkaBrokerWriterProperties") Properties properties,
                                @Value("${kafka.producer.topic}") String topic,
                                IHdfsAdapter hdfsAdapter) {
        this.kafkaBrokerWriter = new KafkaBrokerWriter(properties, Collections.singletonList(topic));
        this.hdfsAdapter = hdfsAdapter;
    }

    @Scheduled(initialDelay = 5000, fixedDelay = Long.MAX_VALUE)
    public void process(){
        try {
            Path path = new Path(hdfsAdapter.getNameNode() + "/bigdata/job_descriptions_sorted.csv");
            FSDataInputStream inputStream = hdfsAdapter.getFileSystem().open(path);

            CSVReader csvReader = new CSVReader(new InputStreamReader(inputStream));
            String[] header = csvReader.readNext();
            if (header == null){
                log.info("CSV file is empty or header is missing");
            }

            String[] record;
            while ((record = csvReader.readNext()) != null){
                JobDataDto jobDataDto = new JobDataDto();
                jobDataDto.setJobId(record[0]);
                jobDataDto.setExperience(record[1]);
                jobDataDto.setQualifications(record[2]);
                jobDataDto.setSalaryRange(record[3]);
                jobDataDto.setLocation(record[4]);
                jobDataDto.setCountry(record[5]);
                jobDataDto.setLatitude(record[6]);
                jobDataDto.setLongitude(record[7]);
                jobDataDto.setWorkType(record[8]);
                jobDataDto.setCompanySize(record[9]);
                jobDataDto.setJobPostingDate(record[10]);
                jobDataDto.setPreference(record[11]);
                jobDataDto.setContactPerson(record[12]);
                jobDataDto.setContact(record[13]);
                jobDataDto.setJobTitle(record[14]);
                jobDataDto.setRole(record[15]);
                jobDataDto.setJobPortal(record[16]);
                jobDataDto.setJobDescription(record[17]);
                jobDataDto.setBenefits(record[18]);
                jobDataDto.setSkills(record[19]);
                jobDataDto.setResponsibilities(record[20]);
                jobDataDto.setCompanyName(record[21]);

                CompanyProfileDto companyProfileDto = Constants.OBJECT_MAPPER.readValue(record[22], CompanyProfileDto.class);
                jobDataDto.setCompanyProfile(companyProfileDto);

                kafkaBrokerWriter.write(Constants.OBJECT_MAPPER.writeValueAsString(jobDataDto));
            }

            inputStream.close();
        } catch (IOException | CsvValidationException e) {
            throw new RuntimeException(e);
        }

    }

    public void sort() throws IOException, CsvException {
        String inputPath = hdfsAdapter.getNameNode() + "/bigdata/job_descriptions.csv";
        String outputPath = hdfsAdapter.getNameNode() + "/bigdata/job_descriptions_sorted.csv";

        FSDataInputStream inputStream = hdfsAdapter.getFileSystem().open(new Path(inputPath));
        CSVReader csvReader = new CSVReader(new InputStreamReader(inputStream));
        List<String[]> allRows = csvReader.readAll();
        csvReader.close();
        inputStream.close();
        String[] header = allRows.removeFirst();
        log.info("Sorting CSV file");
        log.info("Started sorting CSV file");
        allRows.sort(Comparator.comparing(row -> row[10]));
        allRows.addFirst(header);
        log.info("Finished sorting CSV file");

        FSDataOutputStream outputStream = hdfsAdapter.getFileSystem().create(new Path(outputPath));
        CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(outputStream));
        csvWriter.writeAll(allRows);
        csvWriter.close();
        outputStream.close();
    }

}