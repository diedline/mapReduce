package com.diedline.sharedfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class sharedFriendsOneReducer extends Reducer<Text, Text, Text, Text> {
    //A B A D


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder stringBuffer = new StringBuilder();
        for (Text person : values) {
            stringBuffer.append(person).append(",");
        }
        context.write(key,new Text(stringBuffer.toString()));
    }
}
