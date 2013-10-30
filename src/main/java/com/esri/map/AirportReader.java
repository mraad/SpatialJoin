package com.esri.map;

import com.esri.core.geometry.Point;
import com.google.common.io.LineReader;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class AirportReader
{
    // 2177,"Rafic Hariri Intl","Beirut","Lebanon","BEY","OLBA",33.820931,35.488389,87,2,"E"
    final Pattern m_pattern = Pattern.compile("^(\\d+),.+,.+,.+,\"([A-Z]{3})\",.+,(-?\\d+\\.\\d+),(-?\\d+\\.\\d+),.+$");

    public void readAirports(
            final List<Airport> list,
            final InputStream inputStream) throws IOException
    {
        if (inputStream != null)
        {
            final LineReader lineReader = new LineReader(new InputStreamReader(inputStream));
            String line = lineReader.readLine();
            while (line != null)
            {
                final Matcher matcher = m_pattern.matcher(line);
                if (matcher.matches())
                {
                    final int id = Integer.parseInt(matcher.group(1));
                    final String code = matcher.group(2);
                    final double y = Double.parseDouble(matcher.group(3));
                    final double x = Double.parseDouble(matcher.group(4));
                    final Airport airport = new Airport();
                    airport.id = id;
                    airport.code = new Text(code);
                    airport.point = new Point(x, y);
                    list.add(airport);
                }
                line = lineReader.readLine();
            }
        }
    }
}
