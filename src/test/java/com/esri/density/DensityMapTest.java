package com.esri.density;

import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.InputStream;

/**
 */
public class DensityMapTest extends DensityMap
{
    @Override
    public InputStream getInputStream(final JobConf jobConf) throws IOException
    {
        return DensityMapTest.class.getResourceAsStream("/data.json");
    }
}
