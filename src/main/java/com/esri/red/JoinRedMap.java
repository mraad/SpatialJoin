package com.esri.red;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point2D;
import com.esri.core.geometry.Polygon;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 */
abstract class JoinRedMap
        extends MapReduceBase
        implements Mapper<LongWritable, Text, LongWritable, OutputValue>
{
    private final Pattern m_pattern = Pattern.compile(",");
    private final LongWritable m_longWritable = new LongWritable();
    private final Envelope2D m_envelope2D = new Envelope2D();
    private final OutputValue m_outputValue = new OutputValue();

    private double m_xmin;
    private double m_ymin;
    private double m_size;

    @Override
    public void configure(final JobConf jobConf)
    {
        m_xmin = jobConf.getFloat("com.esri.xmin", -180.0F);
        m_ymin = jobConf.getFloat("com.esri.ymin", -90.0F);
        m_size = jobConf.getFloat("com.esri.size", 1.0F);
    }

    public void map(
            final LongWritable key,
            final Text text,
            final OutputCollector<LongWritable, OutputValue> collector,
            final Reporter reporter) throws IOException
    {
        final String[] tokens = m_pattern.split(text.toString());
        if (tokens.length == 5)
        {
            final double x = Double.parseDouble(tokens[1]);
            final double y = Double.parseDouble(tokens[2]);
            final double w = Double.parseDouble(tokens[3]);
            final double h = Double.parseDouble(tokens[4]);
            final Geometry geometry = toGeometry(x, y, w, h);

            m_outputValue.index = getIndex();
            m_outputValue.bytes = GeometryEngine.geometryToEsriShape(geometry);

            geometry.queryEnvelope2D(m_envelope2D);
            final Point2D lowerLeft = m_envelope2D.getLowerLeft();
            final long cmin = (long) Math.max(0.0, Math.floor((lowerLeft.x - m_xmin) / m_size));
            final long rmin = (long) Math.max(0.0, Math.floor((lowerLeft.y - m_ymin) / m_size));
            final Point2D upperRight = m_envelope2D.getUpperRight();
            final long cmax = 1L + (long) Math.max(0.0, Math.floor((upperRight.x - m_xmin) / m_size));
            final long rmax = 1L + (long) Math.max(0.0, Math.floor((upperRight.y - m_ymin) / m_size));

            for (long r = rmin; r < rmax; r++)
            {
                final long rofs = r << 16;
                for (long c = cmin; c < cmax; c++)
                {
                    m_longWritable.set(rofs | c);
                    collector.collect(m_longWritable, m_outputValue);
                }
            }
        }
    }

    abstract byte getIndex();

    public static Geometry toGeometry(
            final double x,
            final double y,
            final double w,
            final double h)
    {
        final Polygon polygon = new Polygon();
        polygon.startPath(x, y);
        polygon.lineTo(x + w, y);
        polygon.lineTo(x + w, y + h);
        polygon.lineTo(x, y + h);
        polygon.closePathWithLine();
        return polygon;
    }

}
