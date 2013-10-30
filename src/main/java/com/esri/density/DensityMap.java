package com.esri.density;

import com.esri.core.geometry.GeoJsonImportFlags;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.Point;
import com.google.common.io.LineReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 */
class DensityMap
        extends MapReduceBase
        implements Mapper<LongWritable, Text, IntWritable, IntWritable>
{
    static IntWritable ONE = new IntWritable(1);

    private final Pattern m_pattern = Pattern.compile(",");
    private final Point m_point = new Point();
    private final IntWritable m_key = new IntWritable();
    private final List<MapGeometry> m_geometryList = new ArrayList<MapGeometry>();

    @Override
    public void configure(final JobConf jobConf)
    {
        try
        {
            final InputStream inputStream = getInputStream(jobConf);
            try
            {
                final LineReader lineReader = new LineReader(new InputStreamReader(inputStream));
                String line = lineReader.readLine();
                while (line != null)
                {
                    final MapGeometry mapGeometry = GeometryEngine.geometryFromGeoJson(line, GeoJsonImportFlags.geoJsonImportDefaults, Geometry.Type.Polygon);
                    m_geometryList.add(mapGeometry);
                    line = lineReader.readLine();
                }
            }
            finally
            {
                inputStream.close();
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    // Public so it can be overwritten in unit test case.
    public InputStream getInputStream(final JobConf jobConf) throws IOException
    {
        final File file = new File("./geojson"); // read from DistributedCache
        return file.toURI().toURL().openStream();
    }

    public void map(
            final LongWritable key,
            final Text text,
            final OutputCollector<IntWritable, IntWritable> collector,
            final Reporter reporter) throws IOException
    {
        final String[] split = m_pattern.split(text.toString());
        if (split.length > 2)
        {
            m_point.setX(Double.parseDouble(split[1]));
            m_point.setY(Double.parseDouble(split[2]));

            int index = 0;
            for (final MapGeometry geometry : m_geometryList)
            {
                if (GeometryEngine.contains(geometry.getGeometry(), m_point, geometry.getSpatialReference()))
                {
                    m_key.set(index);
                    collector.collect(m_key, ONE);
                    break;
                }
                index++;
            }
        }
    }
}
