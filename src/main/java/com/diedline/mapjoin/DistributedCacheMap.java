package com.diedline.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class DistributedCacheMap extends Mapper<LongWritable, Text, Text, NullWritable> {
   private Text t = new Text();

    private HashMap<String, String> map = new HashMap<String, String>();

    /**
     * 先缓存pd表
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取输入字节流
        FileInputStream fis = new FileInputStream("F:\\input\\pd.txt");
        //获取转换流
        InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
        //获取缓存流
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        while (StringUtils.isNotEmpty(line = br.readLine())) {
            String[] fields = line.split("\t");
            //将数据放入hashmap中
            map.put(fields[0], fields[1]);
        }
        fis.close();
        ;
        isr.close();
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //通过pid获取pname
        String[] fields = line.split("\t");
        String pid = fields[1];
        String pname = map.get(pid);
        String str = line + "\t" + pname;
        t.set(str);
        context.write(t, NullWritable.get());
    }


}
