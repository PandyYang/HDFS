package com.pandy.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

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

        configuration.set("dfs.replication", "2");

        String user = "root";

        // 获取客户端对象
        fileSystem = FileSystem.get(uri, configuration, user);


    }

    // 创建文件夹
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

    // 下载文件
    @Test
    public void getFile() throws IOException {
        // 是否删除源文件 删除hdfs上的源文件
        // src 源文件的路径HDFS路径
        // 目标地址路径
        // 是否进行crc校验，false 会有一个crc文件，用于对比确认源文件是否被篡改
        fileSystem.copyToLocalFile(
                false,
                new Path("/xiyou/huaguoshan/log4j.properties"),
                new Path("C:\\Users\\Administrator\\IdeaProjects\\HDFS\\src\\main\\resources\\log4j.properties2"),
                true);
    }

    // 移动文件 和 重命名
    @Test
    public void moveFile() throws IOException {
        // src源文件路径
        // dest 目标文件路径
        fileSystem.rename(
                new Path("/xiyou/huaguoshan/log4j.properties"),
                new Path("/xiyou/huaguoshan/log4j.properties2"));
    }

    @Test
    public void moveFile2() throws IOException {
        // 修改至不同目录下的不同名称
        fileSystem.rename(
                new Path("/xiyou/huaguoshan/log4j.properties2"),
                new Path("/log4j.properties")
        );
    }

    @Test
    public void moveDir() throws IOException {

        // 目录更名
        fileSystem.rename(
                new Path("/input"),
                new Path("/input2")
        );
    }

    @Test
    public void fileDetails() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/"), true);
        while (files.hasNext()) {
            System.out.println("======================================");
            LocatedFileStatus fileStatus = files.next();
            Path path = fileStatus.getPath();
            System.out.println("name = " + path.getName());
            String owner = fileStatus.getOwner();
            System.out.println("owner = " + owner);
            long len = fileStatus.getLen();
            System.out.println("len = " + len);
            long modificationTime = fileStatus.getModificationTime();
            System.out.println("modificationTime = " + modificationTime);
            long blockSize = fileStatus.getBlockSize();
            System.out.println("blockSize = " + blockSize);


            /**
             * 块信息
             * 大文件会分多个不同的块 一个块的大小是128M
             * [0,134217728,hadoop102,hadoop103,hadoop104, 134217728,60795424,hadoop102,hadoop103,hadoop104]
             */
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println("Arrays.toString(blockLocations) = " + Arrays.toString(blockLocations));

            System.out.println("======================================");
        }
    }
}
