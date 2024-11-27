package com.bigdata.it4931.layer.infrastructure.hdfs.impl.write;

import com.bigdata.it4931.layer.infrastructure.hdfs.impl.HdfsAdapter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class HdfsWriter {
    private final FileSystem fileSystem;

    public HdfsWriter(HdfsAdapter hdfsAdapter) {
        this.fileSystem = hdfsAdapter.getFileSystem();
    }

    public void wirteFile(String path, String data) {
        Path hdfsWritePath = new Path(path);
        try (FSDataOutputStream outputStream = fileSystem.create(hdfsWritePath, true)) {
            outputStream.writeBytes(data);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void appendToFile(String path, String data) {
        Path hdfsWritePath = new Path(path);
        try (FSDataOutputStream outputStream = fileSystem.append(hdfsWritePath)) {
            outputStream.writeBytes(data);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
