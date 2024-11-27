package com.bigdata.it4931.layer.infrastructure.hdfs.impl.read;

import com.bigdata.it4931.layer.infrastructure.hdfs.impl.HdfsAdapter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;

@Slf4j
@Component
public class HdfsReader {
    private final FileSystem fileSystem;

    public HdfsReader(HdfsAdapter hdfsAdapter) {
        this.fileSystem = hdfsAdapter.getFileSystem();
    }

    public String readFile(String path) {
        try {
            Path hdfsReadPath = new Path(path);
            if (!fileSystem.exists(hdfsReadPath)) {
                log.error("File does not exist: " + path);
                return null;
            }

            StringBuilder content = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(hdfsReadPath)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    content.append(line).append("\n");
                }
            }
            return content.toString();
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
