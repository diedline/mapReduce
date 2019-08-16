package com.diedline.keyvalue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.soap.Text;
import java.io.IOException;

public class KeyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private LongWritable r = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable a : values
        ) {
            sum += a.get();
        }
        r.set(sum);
        context.write(key, r);
    }
}
