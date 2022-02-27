import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StepsRunner {
    public static void main(String[] args) throws Exception {
        String input = args[0];
        String output = "s3://emrodaiar/output/";
        String time = args[1];
        String output1 = output + "Step1Output" + time + "/";
        System.out.println("Starting job 1");
        Configuration conf1 = new Configuration();
        conf1.set("mapreduce.map.java.opts", "-Xmx512m");
        conf1.set("mapreduce.reduce.java.opts", "-Xmx1536m");
        conf1.set("mapreduce.map.memory.mb", "768");
        conf1.set("mapreduce.reduce.memory.mb", "2048");
        conf1.set("yarn.app.mapreduce.am.resource.mb", "2048");
        conf1.set("yarn.scheduler.minimum-allocation-mb", "256");
        conf1.set("yarn.scheduler.maximum-allocation-mb", "12288");
        conf1.set("yarn.nodemanager.resource.memory-mb", "12288");
        conf1.set("mapreduce.reduce.shuffle.memory.limit.percent", "0.5");

        Job job = Job.getInstance(conf1, "Step1");
        MultipleInputs.addInputPath(job, new Path(input), SequenceFileInputFormat.class,
                Step1.MapperClass.class);
        job.setJarByClass(Step1.class);
        job.setMapperClass(Step1.MapperClass.class);
        job.setPartitionerClass(Step1.PartitionerClass.class);
        job.setCombinerClass(Step1.Combiner.class);
        job.setReducerClass(Step1.ReducerClass.class);
        job.setNumReduceTasks(10);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output1));
        if (job.waitForCompletion(true)) {
            System.out.println("Step 1 finished");
        } else {
            System.out.println("Step 1 failed ");
        }
        //JOB 1 : STATUS: DONE!
        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        System.out.println("Starting job 2");
        String output2 = output + "Step2Output" + time + "/";
        System.out.println("output2 = " + output2);
        Configuration conf2 = new Configuration();
        Counters jobCounters;
        jobCounters = job.getCounters();
        for (CounterGroup counterGroup : jobCounters)
            for (Counter counter : counterGroup)
                if (counter.getName().contains("N_")) {
                    conf2.set(counter.getName(), "" + counter.getValue());
                }
        Job job2 = Job.getInstance(conf2, "Step2");
        job2.setJarByClass(Step2.class);
        job2.setMapperClass(Step2.MapperClass.class);
        job2.setPartitionerClass(Step2.PartitionerClass.class);
        job2.setReducerClass(Step2.ReducerClass.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(10);
        FileInputFormat.setInputPaths(job2, new Path(output1));
        FileOutputFormat.setOutputPath(job2, new Path(output2));
        if (job2.waitForCompletion(true)) {
            System.out.println("Step 2 finished");
        } else {
            System.out.println("Step 2 failed ");
        }
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
        System.out.println("Starting job 3");
        String output3 = output + "Step3Output" + time + "/";
        Configuration conf3 = new Configuration();
        jobCounters = job.getCounters();
        for (CounterGroup counterGroup : jobCounters)
            for (Counter counter : counterGroup)
                if (counter.getName().contains("N_")) {
                    System.out.println("Passing " + counter.getName() + " with value " + counter.getValue() + " to step 3");
                    conf3.set(counter.getName(), "" + counter.getValue());
                }
        Job job3 = Job.getInstance(conf3, "Step3");
        job3.setJarByClass(Step3.class);
        job3.setMapperClass(Step3.MapperClass.class);
        job3.setReducerClass(Step3.ReducerClass.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setNumReduceTasks(10);
        job3.setMapOutputValueClass(Text.class);
        job3.setSortComparatorClass(Step3.DescendingComperator.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        //        job3.setPartitionerClass(Step3.PartitionerClass.class);
        FileInputFormat.addInputPath(job3, new Path(output2));
        FileOutputFormat.setOutputPath(job3, new Path(output3));
        job3.waitForCompletion(true);
        jobCounters = job3.getCounters();
        //Configuration jobConfigs = job3.getConfiguration();
        for (CounterGroup counterGroup : jobCounters)
            for (Counter counter : counterGroup)
                System.out.println("Passing " + counter.getName() + " with value " + counter.getValue() + " to step 3");

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
        System.out.println("All steps are done");
    }
}