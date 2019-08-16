package com.diedline.sharedfriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class shareFriendsTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();
    //a i,k,c,b,g,f,h,o,d
    //b a,f,j,e

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] friend2Person = line.split("\t");
        String[] persons = friend2Person[1].split(",");
        Arrays.sort(persons);
        for (int i = 0; i < persons.length - 1; i++) {
            for (int j = i + 1; j < persons.length; j++) {
                String personToPerson = persons[i] + "-" + persons[j];
                k.set(personToPerson);
                v.set(friend2Person[0]);
                context.write(k, v);
            }
        }
    }
}
