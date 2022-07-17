package com.pandy.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
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

    private FileSystem fileSystem;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://hadoop102:8020");

        Configuration configuration = new Configuration();

        String user = "root";

        // 获取客户端对象
        fileSystem = FileSystem.get(uri, configuration, user);


    }

    @Test
    public void mkdir() throws IOException {

        // 创建一个文件夹操作
        fileSystem.mkdirs(new Path("/xiyou/huaguoshan"));

    }

    @After
    public void close() throws IOException {

        // 关闭资源
        fileSystem.close();
    }

    // 上传操作
    @Test
    public void testPut() throws IOException {

        // delSrc 上传之后是否删除原始数据
        // overwrite 是否允许覆盖
        // src 源数据路径
        // dest 目的地路径
        fileSystem.copyFromLocalFile(
                false,
                true,
                new Path("C:\\Users\\Administrator\\IdeaProjects\\HDFS\\src\\main\\resources\\log4j.properties"),
                new Path("/xiyou/huaguoshan"));
    }
}
