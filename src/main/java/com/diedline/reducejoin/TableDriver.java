package com.diedline.reducejoin;

import com.diedline.wordcount.WordCountDriver;
import com.diedline.wordcount.WordCountMapper;
import com.diedline.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取配置信息，获取job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //2.指定jar包路径
        job.setJarByClass(TableDriver.class);
        //3.关联mapper和reducer类
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);
        //4.指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);
        //5.指定最后输出数据的kv类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(TableBean.class);
        //6.指定job的原始输入文件的目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //7.指定job输出的数据的路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交
        boolean value = job.waitForCompletion(true);
        System.exit(value ? 0:1);
    }
}
