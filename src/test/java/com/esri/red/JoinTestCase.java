package com.esri.red;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

/**
 */
public class JoinTestCase extends ClusterMapReduceTestCase
{
    @Override
    @Before
    public void setUp() throws Exception
    {
    /*
        final File f = new File("build/test/mapred/local").getAbsoluteFile();
        if (f.exists())
        {
            FileUtils.forceDelete(f);
        }
        FileUtils.forceMkdir(f);
        System.setProperty("hadoop.log.dir", f.getAbsolutePath());
        */

        setupMetricsLogging();
        setupDefaultSystemProperties();
        super.setUp();
    }

    public void setupMetricsLogging()
    {
        org.apache.log4j.Logger.getLogger(org.apache.hadoop.metrics2.util.MBeans.class).
                setLevel(org.apache.log4j.Level.ERROR);
        org.apache.log4j.Logger.getLogger(org.apache.hadoop.metrics2.impl.MetricsSystemImpl.class).
                setLevel(org.apache.log4j.Level.ERROR);
    }

    public void setupDefaultSystemProperties()
    {
        if (System.getProperty("hadoop.log.dir") == null)
        {
            System.setProperty("hadoop.log.dir", System.getProperty("java.io.tmpdir", "."));
        }
        if (System.getProperty("hadoop.log.file") == null)
        {
            System.setProperty("hadoop.log.file", "hadoop.log");
        }
        if (System.getProperty("hadoop.root.logger") == null)
        {
            System.setProperty("hadoop.root.logger", "INFO,console");
        }
    }

    @Test
    public void testJoin() throws InterruptedException, IOException
    {
        writeInput("data1.txt", "0,0,0,5,5\n");
        writeInput("data2.txt", "0,2,2,5,5\n");

        final JobConf jobConf = getMRCluster().createJobConf();

        jobConf.setBoolean("mapred.tasktracker.jetty.cpu.check.enabled", false);

        JoinRedTool.setupJobConf(jobConf, JsonJoinRedReduce.class, "data1.txt", "data2.txt", "output");

        JobClient.runJob(jobConf);

        readOutput();
    }

    private void readOutput() throws IOException
    {
        final InputStream inputStream = getFileSystem().open(new Path("output", "part-00000"));
        try
        {
            final JsonFactory jsonFactory = new JsonFactory();
            final JsonParser jsonParser = jsonFactory.createJsonParser(inputStream);
            final MapGeometry mapGeometry = GeometryEngine.jsonToGeometry(jsonParser);
            final Envelope2D envelope2D = new Envelope2D();
            mapGeometry.getGeometry().queryEnvelope2D(envelope2D);
            Assert.assertEquals(2.0, envelope2D.xmin, 0.000001);
            Assert.assertEquals(2.0, envelope2D.ymin, 0.000001);
            Assert.assertEquals(5.0, envelope2D.xmax, 0.000001);
            Assert.assertEquals(5.0, envelope2D.ymax, 0.000001);
        }
        finally
        {
            inputStream.close();
        }
    }

    private void writeInput(
            final String name,
            final String data) throws IOException
    {
        final Writer wr = new OutputStreamWriter(getFileSystem().create(new Path(name)));
        try
        {
            wr.write(data);
        }
        finally
        {
            wr.close();
        }
    }

}
