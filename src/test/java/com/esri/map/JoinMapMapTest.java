package com.esri.map;

import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.InputStream;

/**
 */
public class JoinMapMapTest extends JoinMapMap
{
    @Override
    public InputStream getAirportsInputStream(final JobConf jobConf) throws IOException
    {
        return JoinMapMapTest.class.getResourceAsStream("/airports.csv");
    }
}
