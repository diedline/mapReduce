package com.diedline.logParse;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogParseMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        Boolean result = LogParse(line, context);
        if (!result) {
            return;
        }
        k.set(line);
        context.write(k, NullWritable.get());
    }

    private Boolean LogParse(String line, Context context) {
        String[] words = line.split(" ");
        if (words.length > 11) {
            //使用计数器进行累加统计 第一个值是组名 第二个值是计数器名称
            context.getCounter("map", "true").increment(1);
            return true;
        }
        context.getCounter("map", "false").increment(1);
        return false;
    }
}
