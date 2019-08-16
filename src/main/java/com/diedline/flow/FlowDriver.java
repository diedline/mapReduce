package com.diedline.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //2.获取jar路径
        job.setJarByClass(FlowDriver.class);
        //3.关联mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //4.指定关联mapper kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //5.关联最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //6.设置最终输入文件路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //7.设置最终输出文件路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //8.提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
