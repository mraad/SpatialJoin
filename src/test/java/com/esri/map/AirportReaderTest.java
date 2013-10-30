package com.esri.map;

import com.esri.map.Airport;
import com.esri.map.AirportReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class AirportReaderTest
{
    @Test
    public void testReader() throws IOException
    {
        final AirportReader airportReader = new AirportReader();
        final InputStream inputStream = AirportReaderTest.class.getResourceAsStream("/airports.csv");
        try
        {
            final List<Airport> list = new ArrayList<Airport>();
            airportReader.readAirports(list, inputStream);
            Assert.assertNotNull(list);
            Assert.assertEquals(3, list.size());

            final Airport bey = list.get(0);
            Assert.assertEquals(2177, bey.id);
            Assert.assertEquals("BEY", bey.code.toString());
            Assert.assertEquals(33.820931, bey.point.getY(), 0.0000001);
            Assert.assertEquals(35.488389, bey.point.getX(), 0.0000001);

            final Airport atl = list.get(1);
            Assert.assertEquals(3682, atl.id);
            Assert.assertEquals("ATL", atl.code.toString());
            Assert.assertEquals(33.636719, atl.point.getY(), 0.0000001);
            Assert.assertEquals(-84.428067, atl.point.getX(), 0.0000001);
        }
        finally
        {
            inputStream.close();
        }
    }
}
