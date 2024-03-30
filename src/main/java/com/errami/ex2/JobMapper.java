package com.errami.ex2;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class JobMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String word = value.toString();
        String[] informations = word.split(" ");
        String ipAdd = informations[0].split(" -- ")[0];
        context.write(new Text(ipAdd),new IntWritable(1));
    }
}