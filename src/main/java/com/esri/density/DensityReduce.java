package com.esri.density;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 */
final class DensityReduce
        extends MapReduceBase
        implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
{
    public void reduce(
            final IntWritable key,
            final Iterator<IntWritable> iterator,
            final OutputCollector<IntWritable, IntWritable> collector,
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
