package com.esri.red;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point2D;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class MapRedTest
{
    private MapDriver<
            LongWritable, Text,
            LongWritable, OutputValue
            > m_mapDriver;
    private ReduceDriver<
            LongWritable, OutputValue,
            NullWritable, BytesWritable
            > m_reduceDriver;

    @Before
    public void setUp()
    {
        m_mapDriver = MapDriver.newMapDriver(new JoinRedMap0());
        m_reduceDriver = ReduceDriver.newReduceDriver(new ShapeJoinRedReduce());
    }

    private void setConfigurationProperties(final Configuration configuration)
    {
        configuration.setFloat("com.esri.xmin", -10.0F);
        configuration.setFloat("com.esri.ymin", -10.0F);
        configuration.setFloat("com.esri.xmax", 10.0F);
        configuration.setFloat("com.esri.ymax", 10.0F);
        configuration.setFloat("com.esri.size", 10.0F);
    }

    @Test
    public void testMapperQuad() throws IOException
    {
        final Geometry geometry = JoinRedMap.toGeometry(-5, -5, 10, 10);
        final OutputValue outputValue = new OutputValue();
        outputValue.bytes = GeometryEngine.geometryToEsriShape(geometry);
        setConfigurationProperties(m_mapDriver.getConfiguration());
        m_mapDriver.
                withInput(new LongWritable(0), new Text("0,-5,-5,10,10")).
                withOutput(new LongWritable(0), outputValue).
                withOutput(new LongWritable(1), outputValue).
                withOutput(new LongWritable((1 << 16)), outputValue).
                withOutput(new LongWritable((1 << 16) | 1), outputValue).
                runTest();
    }

    @Test
    public void testMapperQ1() throws IOException
    {
        final Geometry geometry = JoinRedMap.toGeometry(-5, -5, 2, 2);
        final OutputValue outputValue = new OutputValue();
        outputValue.bytes = GeometryEngine.geometryToEsriShape(geometry);
        setConfigurationProperties(m_mapDriver.getConfiguration());
        m_mapDriver.
                withInput(new LongWritable(0), new Text("0,-5,-5,2,2")).
                withOutput(new LongWritable(0), outputValue).
                runTest();
    }

    @Test
    public void testMapperQ2() throws IOException
    {
        final Geometry geometry = JoinRedMap.toGeometry(-5, -5, 10, 2);
        final OutputValue outputValue = new OutputValue();
        outputValue.bytes = GeometryEngine.geometryToEsriShape(geometry);
        setConfigurationProperties(m_mapDriver.getConfiguration());
        m_mapDriver.
                withInput(new LongWritable(0), new Text("0,-5,-5,10,2")).
                withOutput(new LongWritable(0), outputValue).
                withOutput(new LongWritable(1), outputValue).
                runTest();
    }

    @Test
    public void testMapperQ3() throws IOException
    {
        final Geometry geometry = JoinRedMap.toGeometry(-5, -5, 2, 10);
        final OutputValue outputValue = new OutputValue();
        outputValue.bytes = GeometryEngine.geometryToEsriShape(geometry);
        setConfigurationProperties(m_mapDriver.getConfiguration());
        m_mapDriver.
                withInput(new LongWritable(0), new Text("0,-5,-5,2,10")).
                withOutput(new LongWritable(0), outputValue).
                withOutput(new LongWritable(1 << 16), outputValue).
                runTest();
    }

    @Test
    public void testReduce1() throws IOException
    {
        final List<OutputValue> list = new ArrayList<OutputValue>();

        list.add(new OutputValue((byte) 0, GeometryEngine.geometryToEsriShape(JoinRedMap.toGeometry(-2, -6, 4, 2))));
        list.add(new OutputValue((byte) 1, GeometryEngine.geometryToEsriShape(JoinRedMap.toGeometry(-1, -7, 2, 2))));

        setConfigurationProperties(m_reduceDriver.getConfiguration());
        final List<Pair<NullWritable, BytesWritable>> pairList = m_reduceDriver.
                withInput(new LongWritable(0), list).
                run();
        Assert.assertEquals(1, pairList.size());
        final Pair<NullWritable, BytesWritable> pair = pairList.get(0);
        final Geometry geometry = GeometryEngine.geometryFromEsriShape(pair.getSecond().getBytes(), Geometry.Type.Unknown);
        final Envelope2D envelope2D = new Envelope2D();
        geometry.queryEnvelope2D(envelope2D);
        final Point2D point2D = new Point2D();
        envelope2D.queryLowerLeft(point2D);
        Assert.assertEquals(-1, point2D.x, 0.000001);
        Assert.assertEquals(-6, point2D.y, 0.000001);
        Assert.assertEquals(2, envelope2D.getWidth(), 0.000001);
        Assert.assertEquals(1, envelope2D.getHeight(), 0.000001);
    }

    @Test
    public void testReduce2() throws IOException
    {
        final List<OutputValue> list = new ArrayList<OutputValue>();

        list.add(new OutputValue((byte) 0, GeometryEngine.geometryToEsriShape(JoinRedMap.toGeometry(0, 0, 1, 1))));
        list.add(new OutputValue((byte) 1, GeometryEngine.geometryToEsriShape(JoinRedMap.toGeometry(0, 0, 1, 1))));

        final Configuration configuration = m_reduceDriver.getConfiguration();
        configuration.setFloat("com.esri.xmin", 0.0F);
        configuration.setFloat("com.esri.ymin", 0.0F);
        configuration.setFloat("com.esri.xmax", 10.0F);
        configuration.setFloat("com.esri.ymax", 10.0F);
        configuration.setFloat("com.esri.size", 1.0F);

        final List<Pair<NullWritable, BytesWritable>> pairList = m_reduceDriver.
                withInput(new LongWritable(0), list).
                run();
        Assert.assertEquals(1, pairList.size());
        final Pair<NullWritable, BytesWritable> pair = pairList.get(0);
        final Geometry geometry = GeometryEngine.geometryFromEsriShape(pair.getSecond().getBytes(), Geometry.Type.Unknown);
        final Envelope2D envelope2D = new Envelope2D();
        geometry.queryEnvelope2D(envelope2D);
        final Point2D point2D = new Point2D();
        envelope2D.queryLowerLeft(point2D);
        Assert.assertEquals(0, point2D.x, 0.000001);
        Assert.assertEquals(0, point2D.y, 0.000001);
        Assert.assertEquals(1, envelope2D.getWidth(), 0.000001);
        Assert.assertEquals(1, envelope2D.getHeight(), 0.000001);
    }
}
