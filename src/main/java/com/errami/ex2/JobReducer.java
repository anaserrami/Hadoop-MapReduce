package com.errami.ex2;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class JobReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int counter=0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()){
            counter+=iterator.next().get();
        }
        context.write(key,new IntWritable(counter));
    }
}