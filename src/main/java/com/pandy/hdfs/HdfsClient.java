package com.pandy.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 客户端代码
 *
 * 1. 获取客户端对象
 * 2. 执行相关的操作命令
 * 3. 关闭资源
 */
public class HdfsClient {

    @Test
    public void mkdir() throws URISyntaxException, IOException, InterruptedException {

        URI uri = new URI("hdfs://hadoop102:8020");

        Configuration configuration = new Configuration();

        String user = "root";

        // 获取客户端对象
        FileSystem fileSystem = FileSystem.get(uri, configuration, user);

        // 创建一个文件夹操作
        fileSystem.mkdirs(new Path("/xiyou/huaguoshan"));

        // 关闭资源
        fileSystem.close();
    }
}
