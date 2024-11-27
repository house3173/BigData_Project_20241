package com.bigdata.it4931.config.hdfs;

import com.bigdata.it4931.config.hdfs.properties.HdfsProperties;
import com.bigdata.it4931.utility.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

@org.springframework.context.annotation.Configuration
public class HdfsConfiguration {

    @Bean(name = "hdfsProperties")
    @ConfigurationProperties(prefix = "hadoop.hdfs")
    public HdfsProperties getHdfsProperties() {
        return new HdfsProperties();
    }

    @Bean("hdfsSiteConfiguration")
    public Configuration getHdfsSiteConfiguration(@Qualifier("hdfsProperties") HdfsProperties hdfsProperties) {
        return load("hdfs.site.conf", hdfsProperties.getHdfsSiteConf());
    }

    @Bean("coreSiteConfiguration")
    public Configuration getCoreSiteConfiguration(@Qualifier("hdfsProperties") HdfsProperties hdfsProperties) {
        return load("core.site.conf", hdfsProperties.getCoreSiteConf());
    }

    private Configuration load(String envKey, String fileInClassPath) {
        final String path = System.getProperty(envKey);
        if (!StringUtils.isNullOrEmpty(path)) {
            return loadConfig(path);
        }
        final ClassLoader classLoader = HdfsConfiguration.class.getClassLoader();
        return loadConfigStream(classLoader.getResourceAsStream(fileInClassPath));
    }

    private Configuration loadConfig(String path) {
        try (InputStream inputStream = Files.newInputStream(Path.of(path))) {
            return loadConfigStream(inputStream);
        } catch (IOException e) {
            return new Configuration();
        }
    }

    private Configuration loadConfigStream(InputStream inputStream) {
        Configuration configuration = new Configuration();
        try {
            configuration.addResource(inputStream);
        } catch (Exception ignored) {
        }
        return configuration;
    }
}
