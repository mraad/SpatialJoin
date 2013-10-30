package com.esri.red;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point2D;
import com.esri.core.geometry.Polygon;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 */
public class OutputValueTest
{

    @Test
    public void testOutputValue()
    {
        final byte[] bytes = GeometryEngine.geometryToEsriShape(JoinRedMap.toGeometry(-10, -10, 20, 20));
        final OutputValue outputValue = new OutputValue((byte) 12, bytes);
        Assert.assertArrayEquals(bytes, outputValue.bytes);
        Assert.assertEquals(12, outputValue.index);
    }

    @Test
    public void testWriteRead() throws Exception
    {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        final OutputValue writeValue = new OutputValue((byte) 12, GeometryEngine.geometryToEsriShape(JoinRedMap.toGeometry(-10, -10, 20, 20)));
        writeValue.write(new DataOutputStream(byteArrayOutputStream));

        final OutputValue readValue = new OutputValue();
        readValue.readFields(new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray())));

        Assert.assertEquals(12, readValue.index);

        final Geometry readGeom = GeometryEngine.geometryFromEsriShape(readValue.bytes, Geometry.Type.Unknown);

        Assert.assertTrue(readGeom instanceof Polygon);

        final Envelope2D envelope2D = new Envelope2D();
        readGeom.queryEnvelope2D(envelope2D);

        final Point2D point2D = new Point2D();

        envelope2D.queryLowerLeft(point2D);
        Assert.assertEquals(-10.0, point2D.x, 0.000001);
        Assert.assertEquals(-10.0, point2D.y, 0.000001);

        envelope2D.queryUpperRight(point2D);
        Assert.assertEquals(10.0, point2D.x, 0.000001);
        Assert.assertEquals(10.0, point2D.y, 0.000001);

    }
}
