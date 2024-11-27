package com.bigdata.it4931.layer.infrastructure.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public interface IHdfsAdapter {
    Configuration getConfiguration();
    FileSystem getFileSystem();
    String getNameNode();
}
