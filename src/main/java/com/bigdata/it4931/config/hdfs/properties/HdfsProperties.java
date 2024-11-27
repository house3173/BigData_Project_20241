package com.bigdata.it4931.config.hdfs.properties;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HdfsProperties {
    private String user;
    private String hdfsSiteConf;
    private String coreSiteConf;
}
