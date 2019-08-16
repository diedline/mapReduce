package com.diedline.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


import javax.xml.soap.Text;
import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream testOut = null;
    private FSDataOutputStream otherOut = null;
    private FileSystem fileSystem = null;

    public LogRecordWriter(TaskAttemptContext taskAttemptContext) {
        Configuration configuration = taskAttemptContext.getConfiguration();
        try {
            //获取客户端
            fileSystem = FileSystem.get(configuration);
            Path path = new Path("f:/test.txt");
            Path path1 = new Path("f:/other.txt");
            //输出流
            testOut = fileSystem.create(path);
            otherOut = fileSystem.create(path1);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        //获取数据
        String line = text.toString();
        //判断是否包含atguigu
        if(line.contains("atguigu")){
            testOut.write(line.getBytes());
        }else {
            otherOut.write(line.getBytes());
        }
    }

    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(testOut);
        IOUtils.closeStream(otherOut);
        fileSystem.close();
    }
}
