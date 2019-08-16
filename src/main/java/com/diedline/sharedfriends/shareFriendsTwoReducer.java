package com.diedline.sharedfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class shareFriendsTwoReducer extends Reducer<Text, Text, Text, Text> {
    //a-b c a-b d a-b e
    //a-b c d e


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (Text friend:values){
            sb.append(friend).append("\t");
        }
        context.write(key,new Text(sb.toString()));
    }
}
