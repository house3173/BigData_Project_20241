package com.bigdata.it4931.config.hdfs;

import com.bigdata.it4931.config.hdfs.properties.HdfsProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.*;

@Configuration
public class HdfsConfiguration {

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
