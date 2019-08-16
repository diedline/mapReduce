package com.diedline.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 相当于是yarn的客户端 负责提交mapReduce
 *
 *
 * */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取配置信息，获取job对象实例
        Configuration configuration = new Configuration();

        //开启map端输出压缩
        configuration.setBoolean("mapreduce.map.output.compress", true);
        //设置map段输出压缩方式
        configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        Job job = Job.getInstance(configuration);
        //2.指定jar包路径
        job.setJarByClass(WordCountDriver.class);
        //3.关联mapper和reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4.指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.指定最后输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //reduce端开启压缩
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job,BZip2Codec.class);

        //6.指定job的原始输入文件的目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //7.指定job输出的数据的路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //指定合并小文件
        job.setInputFormatClass(CombineFileInputFormat.class);
        //设置最大切片和最小切片大小
        CombineFileInputFormat.setMaxInputSplitSize(job,4194304);
        CombineFileInputFormat.setMinInputSplitSize(job,2097152);
        //提交
        boolean value = job.waitForCompletion(true);
        System.exit(value ? 0:1);
    }
}
