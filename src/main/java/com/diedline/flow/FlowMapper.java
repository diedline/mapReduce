package com.diedline.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private FlowBean flowBean = new FlowBean();
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //切割，获取相应数据
        String[] fields = line.split("\t");
        //手机号
        String phone = fields[1];
        //上行流量
        long upflow = Long.parseLong(fields[fields.length - 3]);
        //下行流量
        long downflow = Long.parseLong(fields[fields.length - 2]);
        //封装对象
        flowBean.set(upflow,downflow);
        k.set(phone);
        context.write(k, flowBean);
    }
}
