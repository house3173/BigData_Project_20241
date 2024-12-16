package com.bigdata.it4931.config.hdfs;

import com.bigdata.it4931.config.hdfs.properties.HdfsProperties;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.*;

@Getter
@Configuration
public class HdfsConfiguration {
    @Value("${hadoop.hdfs.folder}")
    private String hdfsFolder;

    @Value("${hadoop.hdfs.file.max-size.MB}")
    private int hdfsFileMaxSize;

    @Bean(name = "hdfsProperties")
    @ConfigurationProperties(prefix = "hadoop.hdfs")
    public HdfsProperties getHdfsProperties() {
        return new HdfsProperties();
    }

    @Bean("hdfsSiteInputStream")
    public InputStream getHdfsSiteInputStream(@Qualifier("hdfsProperties") HdfsProperties hdfsProperties) throws FileNotFoundException {
        return new FileInputStream(hdfsProperties.getHdfsSiteConf());
    }

    @Bean("coreSiteInputStream")
    public InputStream getCoreSiteInputStream(@Qualifier("hdfsProperties") HdfsProperties hdfsProperties) throws FileNotFoundException {
        return new FileInputStream(hdfsProperties.getCoreSiteConf());
    }
}
