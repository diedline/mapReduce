package com.diedline.reverseindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReverseIndexOneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text keys = new Text();
    private IntWritable v = new IntWritable(1);
    private String name;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        name = fileSplit.getPath().getName();
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            String str = word + "--" + name;
            keys.set(str);
            context.write(keys, v);
        }
    }
}
