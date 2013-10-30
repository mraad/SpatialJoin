package com.esri.map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 */
final class JoinMapReduce
        extends MapReduceBase
        implements Reducer<Text, IntWritable, Text, IntWritable>
{
    public void reduce(
            final Text key,
            final Iterator<IntWritable> iterator,
            final OutputCollector<Text, IntWritable> collector,
            final Reporter reporter) throws IOException
    {
        int sum = 0;
        while (iterator.hasNext())
        {
            sum += iterator.next().get();
        }
        collector.collect(key, new IntWritable(sum));
    }
}
