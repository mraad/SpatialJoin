package com.esri.red;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;

/**
 */
public class GeoJsonJoinRedReduce<V3> extends AbstractJoinRedReduce<Text>
{
    private final Text m_text = new Text();

    @Override
    protected void collect(
            final OutputCollector<NullWritable, Text> collector,
            final Geometry geometry) throws IOException
    {
        m_text.set(GeometryEngine.geometryToGeoJson(m_spatialReference, geometry));
        collector.collect(NullWritable.get(), m_text);
    }
}
