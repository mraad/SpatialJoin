package com.esri.density;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 */
public class DensityTool extends Configured implements Tool
{
    public static void main(final String[] args) throws Exception
    {
        System.exit(ToolRunner.run(new Configuration(), new DensityTool(), args));
    }

    @Override
    public int run(final String[] args) throws Exception
    {
        final int rc;
        final JobConf jobConf = new JobConf(getConf(), DensityTool.class);
        if (args.length != 3)
        {
            System.err.println("Expecting arguments: input-path file:///tmp/polygons.txt output");
            ToolRunner.printGenericCommandUsage(System.err);
            rc = -1;
        }
        else
        {
            jobConf.setJobName(DensityTool.class.getSimpleName());

            jobConf.setMapperClass(DensityMap.class);
            jobConf.setMapOutputKeyClass(IntWritable.class);
            jobConf.setMapOutputValueClass(IntWritable.class);

            jobConf.setCombinerClass(DensityReduce.class);

            jobConf.setReducerClass(DensityReduce.class);
            jobConf.setOutputKeyClass(IntWritable.class);
            jobConf.setOutputValueClass(IntWritable.class);

            jobConf.setInputFormat(TextInputFormat.class);
            jobConf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(jobConf, new Path(args[0]));

            DistributedCache.createSymlink(jobConf);
            // the text after the # will be used as the "local" file name on each node.
            DistributedCache.addCacheFile(new URI(args[1] + "#geojson"), jobConf);

            final Path outputDir = new Path(args[2]);
            outputDir.getFileSystem(jobConf).delete(outputDir, true);
            FileOutputFormat.setOutputPath(jobConf, outputDir);

            JobClient.runJob(jobConf);
            rc = 0;
        }
        return rc;
    }
}
