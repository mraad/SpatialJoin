package com.esri.map;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryCursor;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Line;
import com.esri.core.geometry.OperatorBuffer;
import com.esri.core.geometry.QuadTree;
import com.esri.core.geometry.SpatialReference;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
class JoinMapMap
        extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, IntWritable>
{
    static IntWritable ONE = new IntWritable(1);

    // 2B,410,LED,2948,NBC,6969,,0,CR2
    // ^.+,.+,([A-Z]{3}),\d+,([A-Z]{3}),.+$
    private final Pattern m_pattern = Pattern.compile("^.+,.+,([A-Z]{3}),.+,([A-Z]{3}),.+$");

    private List<Airport> m_airportList = new ArrayList<Airport>();
    private Map<String, Airport> m_airportMap = new HashMap<String, Airport>();
    private OperatorBuffer m_operatorBuffer = OperatorBuffer.local();
    private SpatialReference m_spatialReference;
    private QuadTree m_quadTree;
    private final Line m_line = new Line();
    private LineCursor m_lineCursor = new LineCursor();
    private double[] m_distances = new double[]{0.00001};
    private QuadTree.QuadTreeIterator m_quadTreeIterator;

    private final class LineCursor extends GeometryCursor
    {
        private int m_index = -1;

        @Override
        public Geometry next()
        {
            m_index++;
            return m_index == 0 ? m_line : null;
        }

        @Override
        public int getGeometryID()
        {
            return m_index;
        }

        public void reset()
        {
            m_index = -1;
        }
    }

    @Override
    public void configure(final JobConf jobConf)
    {
        m_distances[0] = jobConf.getFloat("com.esri.distance", 0.0001F);
        m_spatialReference = SpatialReference.create(jobConf.getInt("com.esri.wkid", 4326));

        final float xmin = jobConf.getFloat("com.esri.xmin", -180.0F);
        final float ymin = jobConf.getFloat("com.esri.ymin", -90.0F);
        final float xmax = jobConf.getFloat("com.esri.xmax", 180.0F);
        final float ymax = jobConf.getFloat("com.esri.ymax", 90.0F);
        final int depth = jobConf.getInt("com.esri.depth", 8);
        m_quadTree = new QuadTree(new Envelope2D(xmin, ymin, xmax, ymax), depth);
        m_quadTreeIterator = m_quadTree.getIterator();
        try
        {
            final InputStream inputStream = getAirportsInputStream(jobConf);
            final AirportReader airportReader = new AirportReader();
            try
            {
                airportReader.readAirports(m_airportList, inputStream);
            }
            finally
            {
                inputStream.close();
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        loadHashMap();
    }

    private void loadHashMap()
    {
        int index = 0;
        for (final Airport airport : m_airportList)
        {
            m_airportMap.put(airport.code.toString(), airport);

            final Envelope2D envelope2D = new Envelope2D();
            airport.point.queryEnvelope2D(envelope2D);
            m_quadTree.insert(index++, envelope2D);
        }
    }

    public InputStream getAirportsInputStream(final JobConf jobConf) throws IOException
    {
        final String location = jobConf.get("com.esri.airports", "airports.csv");
        final Path path = new Path(location);
        return path.getFileSystem(jobConf).open(path);
    }

    public void map(
            final LongWritable key,
            final Text text,
            final OutputCollector<Text, IntWritable> collector,
            final Reporter reporter) throws IOException
    {
        final Envelope2D envelope2D = new Envelope2D();
        final Matcher matcher = m_pattern.matcher(text.toString());
        if (matcher.matches())
        {
            final Airport orig = m_airportMap.get(matcher.group(1));
            final Airport dest = m_airportMap.get(matcher.group(2));
            if (orig != null && dest != null)
            {
                // TODO - use great circle calculation
                m_line.setStart(orig.point);
                m_line.setEnd(dest.point);

                m_lineCursor.reset();
                final GeometryCursor outputCursor = m_operatorBuffer.execute(
                        m_lineCursor,
                        m_spatialReference,
                        m_distances, false, null);
                final Geometry geometry = outputCursor.next();

                // TODO - see it it makes a difference to accelerate / de-accelerate !!
                m_operatorBuffer.accelerateGeometry(geometry, m_spatialReference, Geometry.GeometryAccelerationDegree.enumHot);
                geometry.queryEnvelope2D(envelope2D);
                m_quadTreeIterator.resetIterator(envelope2D, 0.0000001); // TODO - make configurable
                int elementIndex = m_quadTreeIterator.next();
                while (elementIndex >= 0)
                {
                    final int featureIndex = m_quadTree.getElement(elementIndex);
                    final Airport airport = m_airportList.get(featureIndex);
                    if (GeometryEngine.contains(geometry, airport.point, m_spatialReference))
                    {
                        collector.collect(airport.code, ONE);
                    }
                    elementIndex = m_quadTreeIterator.next();
                }
                OperatorBuffer.deaccelerateGeometry(geometry);
            }
        }
    }
}
