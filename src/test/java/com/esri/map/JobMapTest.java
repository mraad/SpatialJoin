package com.esri.map;

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
public class JobMapTest
{
    private MapDriver<
            LongWritable, Text,
            Text, IntWritable
            > m_mapDriver;
    private ReduceDriver<
            Text, IntWritable,
            Text, IntWritable
            > m_reduceDriver;
    private MapReduceDriver<
            LongWritable, Text,
            Text, IntWritable,
            Text, IntWritable
            > m_mapReduceDriver;

    @Before
    public void setUp()
    {
        final JoinMapMap joinMapMap = new JoinMapMapTest();

        final JoinMapReduce joinMapReduce = new JoinMapReduce();

        m_mapDriver = MapDriver.newMapDriver(joinMapMap);

        m_reduceDriver = ReduceDriver.newReduceDriver(joinMapReduce);

        m_mapReduceDriver = MapReduceDriver.newMapReduceDriver(joinMapMap, joinMapReduce);
    }

    @Test
    public void testMapper() throws IOException
    {
        m_mapDriver.
                withInput(new LongWritable(0), new Text("2B,410,BEY,2948,ATL,6969,,0,CR2")).
                withOutput(new Text("BEY"), new IntWritable(1)).
                withOutput(new Text("ATL"), new IntWritable(1)).
                runTest();
    }

    @Test
    public void testReducer() throws IOException
    {
        final List<IntWritable> list = new ArrayList<IntWritable>();
        list.add(JoinMapMap.ONE);
        list.add(JoinMapMap.ONE);

        m_reduceDriver.
                withInput(new Text("BEY"), list).
                withOutput(new Text("BEY"), new IntWritable(2)).
                runTest();
    }

    @Test
    public void testMapReduce() throws IOException
    {
        m_mapReduceDriver.
                withInput(new LongWritable(0), new Text("2B,410,BEY,2948,ATL,6969,,0,747")).
                withInput(new LongWritable(1), new Text("2B,410,BEY,2948,CDG,6969,,0,737")).
                withOutput(new Text("ATL"), new IntWritable(1)).
                withOutput(new Text("BEY"), new IntWritable(2)).
                withOutput(new Text("CDG"), new IntWritable(1)).
                runTest();
    }

}
