package com.diedline.sharedfriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class sharedFriendsOneMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();

    // a:b,c,d,e,f,g
    // b:a,c,e,k


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(":");
        String[] friends = split[1].split(",");
        for (String friend : friends) {
            k.set(friend);
            v.set(split[0]);
            context.write(k, v);
        }
    }
}
