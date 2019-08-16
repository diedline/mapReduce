package com.diedline.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;
import java.io.IOException;

public class LogOutputFormat extends FileOutputFormat<Text, NullWritable> {

    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        RecordWriter<Text,NullWritable> recordWriter = new LogRecordWriter(taskAttemptContext);
        return recordWriter;
    }
}
