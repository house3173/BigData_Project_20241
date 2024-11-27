package com.bigdata.it4931.layer.infrastructure.hdfs.impl;

import com.bigdata.it4931.config.hdfs.properties.HdfsProperties;
import com.bigdata.it4931.layer.infrastructure.hdfs.IHdfsAdapter;
import com.bigdata.it4931.utility.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class HdfsAdapter implements IHdfsAdapter {
    private final Configuration configuration;
    private final String defaultFS;

    public HdfsAdapter(@Qualifier("hdfsProperties") HdfsProperties hdfsProperties,
                       @Qualifier("hdfsSiteConfiguration") Configuration hdfsSiteConfiguration,
                       @Qualifier("coreSiteConfiguration") Configuration coreSiteConfiguration) {
        if (!StringUtils.isNullOrEmpty(hdfsProperties.getUser())) {
            System.setProperty("HADOOP_USER_NAME", hdfsProperties.getUser());
        }
        this.configuration = new Configuration();
        this.configuration.addResource(hdfsSiteConfiguration);
        this.configuration.addResource(coreSiteConfiguration);
        this.defaultFS = this.configuration.get("fs.defaultFS");
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public FileSystem getFileSystem() {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(configuration);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return fs;
    }

    @Override
    public String getNameNode() {
        return defaultFS;
    }
}
