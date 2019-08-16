package com.diedline.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 规范：
 * (1) 用户自定义的Mapper要继承自己的父类
 * (2) Mapper的输入数据是KV对的形式（KV的类型可自定义）
 * (3) Mapper的业务逻辑方法写在map()方法中
 * (4) mapper的输出形式是KV对的形式（KV的类型可自定义）
 * (5) map()方法（mapTask进程）对每一个<K,V>调用一次
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text t = new Text();
    private IntWritable i = new IntWritable(1);

    /**
     * map()方法（mapTask进程）对每一个<K,V>调用一次
     *
     * @param key:数据的offset
     * @param value:要处理的一行数据
     * @param context:上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.将一行转换成String类型
        String line = value.toString();
        //2.切割
        String[] words = line.split(" ");
        //3.循环写出
        for (String word : words) {
            t.set(word);
            context.write(t, i);
        }
    }
}
