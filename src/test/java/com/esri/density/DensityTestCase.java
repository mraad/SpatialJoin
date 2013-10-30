package com.esri.density;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class DensityTestCase
{
    private MapDriver<
            LongWritable, Text,
            IntWritable, IntWritable
            > m_mapDriver;
    private ReduceDriver<
            IntWritable, IntWritable,
            IntWritable, IntWritable
            > m_reduceDriver;
    private MapReduceDriver<
            LongWritable, Text,
            IntWritable, IntWritable,
            IntWritable, IntWritable
            > m_mapReduceDriver;

    @Before
    public void setUp()
    {
        final DensityMap mapper = new DensityMapTest();
        final DensityReduce reducer = new DensityReduce();

        m_mapDriver = MapDriver.newMapDriver(mapper);
        m_reduceDriver = ReduceDriver.newReduceDriver(reducer);
        m_mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException
    {
        m_mapDriver.
                withInput(new LongWritable(0), new Text("0,5,5")).
                withOutput(new IntWritable(0), new IntWritable(1)).
                runTest();
    }

    @Test
    public void testReducer() throws IOException
    {
        final List<IntWritable> list = new ArrayList<IntWritable>();
        list.add(DensityMap.ONE);
        list.add(DensityMap.ONE);

        m_reduceDriver.
                withInput(new IntWritable(0), list).
                withOutput(new IntWritable(0), new IntWritable(2)).
                runTest();
    }

    @Test
    public void testMapReduce() throws IOException
    {
        m_mapReduceDriver.
                withInput(new LongWritable(0), new Text("0,5,5")).
                withInput(new LongWritable(0), new Text("0,6,6")).
                withOutput(new IntWritable(0), new IntWritable(2)).
                runTest();
    }
}
