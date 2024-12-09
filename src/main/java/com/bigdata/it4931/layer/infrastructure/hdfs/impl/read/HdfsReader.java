package com.bigdata.it4931.layer.infrastructure.hdfs.impl.read;

import com.bigdata.it4931.layer.infrastructure.hdfs.impl.HdfsAdapter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;

@Slf4j
@Component
public class HdfsReader {
    private final FileSystem fileSystem;
    private final HdfsAdapter hdfsAdapter;

    public HdfsReader(HdfsAdapter hdfsAdapter) {
        this.hdfsAdapter = hdfsAdapter;
        this.fileSystem = hdfsAdapter.getFileSystem();
    }

    public Object readFile(String path) {
        try {
            Path hdfsReadPath = new Path(hdfsAdapter.getNameNode() + path);
            FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            IOUtils.copyBytes(inputStream, stream, 4096);
            return stream.toByteArray();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    public boolean exists(String path) {
        try {
            return fileSystem.exists(new Path(path));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return false;
    }
}
