package com.esri.red;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point2D;
import com.esri.core.geometry.QuadTree;
import com.esri.core.geometry.SpatialReference;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 */
public abstract class AbstractJoinRedReduce<V3>
        extends MapReduceBase
        implements Reducer<LongWritable, OutputValue, NullWritable, V3>
{
    final class GeomEnvp
    {
        Geometry geometry;
        Envelope2D envelope2D;
    }

    protected QuadTree m_quadTree;
    protected QuadTree.QuadTreeIterator m_quadTreeIterator;
    protected SpatialReference m_spatialReference;
    protected double m_precision;
    protected double m_xmin;
    protected double m_ymin;
    protected double m_xmax;
    protected double m_ymax;
    protected double m_size;

    @Override
    public void configure(final JobConf jobConf)
    {
        m_spatialReference = SpatialReference.create(jobConf.getInt("com.esri.wkid", 4326));
        m_precision = jobConf.getFloat("com.esri.precision", 0.0000001F);
        m_xmin = jobConf.getFloat("com.esri.xmin", -180.0F);
        m_ymin = jobConf.getFloat("com.esri.ymin", -90.0F);
        m_xmax = jobConf.getFloat("com.esri.xmax", 180.0F);
        m_ymax = jobConf.getFloat("com.esri.ymax", 90.0F);
        m_size = jobConf.getFloat("com.esri.size", 1.0F);

        final int depth = jobConf.getInt("com.esri.depth", 8);
        m_quadTree = new QuadTree(new Envelope2D(m_xmin, m_ymin, m_xmax, m_ymax), depth);
        m_quadTreeIterator = m_quadTree.getIterator();
    }

    public void reduce(
            final LongWritable key,
            final Iterator<OutputValue> iterator,
            final OutputCollector<NullWritable, V3> collector,
            final Reporter reporter) throws IOException
    {
        doReduce(key, iterator, collector);
    }

    private void doReduce(
            final LongWritable key,
            final Iterator<OutputValue> iterator,
            final OutputCollector<NullWritable, V3> collector) throws IOException
    {
        final long row = key.get() >> 16;
        final long col = key.get() & 0x7FFF;

        // Current cell bounds
        final double xmin = m_xmin + col * m_size;
        final double ymin = m_ymin + row * m_size;
        final double xmax = xmin + m_size;
        final double ymax = ymin + m_size;

        final List<GeomEnvp> geometryList0 = new ArrayList<GeomEnvp>();
        final List<GeomEnvp> geometryList1 = new ArrayList<GeomEnvp>();

        int index = 0;
        while (iterator.hasNext())
        {
            final OutputValue outputValue = iterator.next();

            final GeomEnvp geomEnvp = new GeomEnvp();
            geomEnvp.geometry = GeometryEngine.geometryFromEsriShape(outputValue.bytes, Geometry.Type.Unknown);
            geomEnvp.envelope2D = new Envelope2D();
            geomEnvp.geometry.queryEnvelope2D(geomEnvp.envelope2D);

            if (outputValue.index == 0)
            {
                geometryList0.add(geomEnvp);
            }
            else
            {
                geometryList1.add(geomEnvp);
                m_quadTree.insert(index++, geomEnvp.envelope2D);
            }
        }
        if (geometryList0.size() > 0 && geometryList1.size() > 0)
        {
            final Point2D point0 = new Point2D();
            final Point2D point1 = new Point2D();
            for (final GeomEnvp geomEnvp0 : geometryList0)
            {
                geomEnvp0.envelope2D.queryLowerLeft(point0);
                m_quadTreeIterator.resetIterator(geomEnvp0.envelope2D, m_precision);
                int elementIndex = m_quadTreeIterator.next();
                while (elementIndex >= 0)
                {
                    final int featureIndex = m_quadTree.getElement(elementIndex);
                    if (featureIndex < geometryList1.size())
                    {
                        final GeomEnvp geomEnvp1 = geometryList1.get(featureIndex);
                        geomEnvp1.envelope2D.queryLowerLeft(point1);

                        // Find union point of the two MBRs
                        final double x = Math.max(point0.x, point1.x);
                        final double y = Math.max(point0.y, point1.y);
                        // Do multi output elimination by checking if union point is in the cell MBR
                        if (xmin <= x && ymin <= y && x < xmax && y < ymax)
                        {
                            // Perform a quick disjoint before finding out if geometries truly intersect
                            if (GeometryEngine.disjoint(geomEnvp0.geometry, geomEnvp1.geometry, m_spatialReference) == false)
                            {
                                collect(collector, GeometryEngine.intersect(geomEnvp0.geometry, geomEnvp1.geometry, m_spatialReference));
                            }
                        }
                    }
                    elementIndex = m_quadTreeIterator.next();
                }
            }
        }
    }

    protected abstract void collect(
            final OutputCollector<NullWritable, V3> collector,
            final Geometry geometry) throws IOException;

}
