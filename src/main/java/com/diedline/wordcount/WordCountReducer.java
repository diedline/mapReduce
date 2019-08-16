package com.diedline.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 规范：
 * (1) 用户自定义的Reduce要继承自己的父类
 * (2) Reduce的输入数据对应Mapper的输入数据
 * (3) Reduce的业务逻辑写在Reduce方法中
 * (5) Reduce进程 对每一个<K,V>调用一次方法
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable val = new IntWritable();
    /**
     * <diedline,1>,<diedline,1>,<diedline,1>,<diedline,1>
     * <hadoop,1>,<hadoop,1>,<hadoop,1>,<hadoop,1>,<hadoop,1>,<hadoop,1>
     * @param key: 单词
     * @param values：单词个数（1）集合
     * @param context： 上下文
     *
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1.累加
        int sum = 0;
        for (IntWritable value:values
             ) {
            sum += value.get();
        }

        //返回值
        val.set(sum);
        context.write(key, val);
    } 
}
