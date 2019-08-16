package com.diedline.reducejoin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    private Text t = new Text();
    private TableBean tableBean = new TableBean();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取文件名称
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        //判断
        String line = value.toString();
        //如果是order表
        if (fileName.startsWith("order")) {
            String[] fields = line.split("\t");
            tableBean.setOldId(fields[0]);
            tableBean.setPid(fields[1]);
            tableBean.setAmount(Integer.parseInt(fields[2]));
            tableBean.setpName("");
            tableBean.setFlag("0");
            t.set(fields[1]);
            context.write(t, tableBean);
        } else {
            String[] fields = line.split("\t");
            tableBean.setOldId("");
            tableBean.setOldId(fields[0]);
            tableBean.setAmount(0);
            tableBean.setpName(fields[1]);
            tableBean.setFlag("1");
            t.set(fields[0]);
            context.write(t, tableBean);
        }

    }

}
