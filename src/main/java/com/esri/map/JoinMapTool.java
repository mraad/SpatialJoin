package com.esri.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 */
public class JoinMapTool extends Configured implements Tool
{
    public static void main(final String[] args) throws Exception
    {
        System.exit(ToolRunner.run(new Configuration(), new JoinMapTool(), args));
    }

    @Override
    public int run(final String[] args) throws Exception
    {
        final int rc;
        final JobConf jobConf = new JobConf(getConf(), JoinMapTool.class);
        if (args.length != 2)
        {
            ToolRunner.printGenericCommandUsage(System.err);
            rc = -1;
        }
        else
        {
            jobConf.setJobName(JoinMapTool.class.getSimpleName());

            jobConf.setMapperClass(JoinMapMap.class);
            jobConf.setMapOutputKeyClass(Text.class);
            jobConf.setMapOutputValueClass(IntWritable.class);

            jobConf.setCombinerClass(JoinMapReduce.class);

            jobConf.setReducerClass(JoinMapReduce.class);
            jobConf.setOutputKeyClass(Text.class);
            jobConf.setOutputValueClass(IntWritable.class);

            jobConf.setInputFormat(TextInputFormat.class);
            jobConf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
            final Path outputDir = new Path(args[1]);
            outputDir.getFileSystem(jobConf).delete(outputDir, true);
            FileOutputFormat.setOutputPath(jobConf, outputDir);

            JobClient.runJob(jobConf);
            rc = 0;
        }
        return rc;
    }
}
