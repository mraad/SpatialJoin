package com.esri.red;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;

/**
 */
final class ShapeJoinRedReduce extends AbstractJoinRedReduce<BytesWritable>
{
    private final BytesWritable m_bytesWritable = new BytesWritable();

    @Override
    protected void collect(
            final OutputCollector<NullWritable, BytesWritable> collector,
            final Geometry geometry) throws IOException
    {
        final byte[] bytes = GeometryEngine.geometryToEsriShape(geometry);
        m_bytesWritable.set(bytes, 0, bytes.length);
        collector.collect(NullWritable.get(), m_bytesWritable);
    }
}
