package com.esri.red;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 */
public class JoinRedTool extends Configured implements Tool
{
    public static void main(final String[] args) throws Exception
    {
        System.exit(ToolRunner.run(new Configuration(), new JoinRedTool(), args));
    }

    @Override
    public int run(final String[] args) throws Exception
    {
        final int rc;

        final JobConf jobConf = new JobConf(getConf());

        final GenericOptionsParser genericOptionsParser = new GenericOptionsParser(jobConf, args);
        final String[] remArgs = genericOptionsParser.getRemainingArgs();

        if (remArgs.length != 3)
        {
            System.err.println("Missing arguments: input1 input2 output");
            ToolRunner.printGenericCommandUsage(System.err);
            rc = -1;
        }
        else
        {
            setupJobConf(jobConf, GeoJsonJoinRedReduce.class, remArgs);
            JobClient.runJob(jobConf);
            rc = 0;
        }
        return rc;
    }

    public static void setupJobConf(
            final JobConf jobConf,
            final Class<? extends AbstractJoinRedReduce> reducerClass,
            final String... args) throws IOException
    {
        jobConf.setJarByClass(JoinRedTool.class);
        jobConf.setJobName(JoinRedTool.class.getSimpleName());

        MultipleInputs.addInputPath(jobConf, new Path(args[0]), TextInputFormat.class, JoinRedMap0.class);
        MultipleInputs.addInputPath(jobConf, new Path(args[1]), TextInputFormat.class, JoinRedMap1.class);

        jobConf.setMapOutputKeyClass(LongWritable.class);
        jobConf.setMapOutputValueClass(OutputValue.class);

        jobConf.setReducerClass(reducerClass);
        jobConf.setOutputKeyClass(NullWritable.class);
        jobConf.setOutputValueClass(Writable.class);

        // TODO configure
        // jobConf.setNumMapTasks(6);
        // jobConf.setNumReduceTasks(6);
        // jobConf.setNumTasksToExecutePerJvm(-1);

        jobConf.setOutputFormat(TextOutputFormat.class);

        final Path outputDir = new Path(args[2]);
        outputDir.getFileSystem(jobConf).delete(outputDir, true);
        FileOutputFormat.setOutputPath(jobConf, outputDir);
    }
}
