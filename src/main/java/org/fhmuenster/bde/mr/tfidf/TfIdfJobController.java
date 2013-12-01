package org.fhmuenster.bde.mr.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TFIDFJobController extends Configured implements Tool {

  private static final String OUTPUT_PATH = "1-word-freq";
  private static final String OUTPUT_PATH_2 = "2-word-counts";

  public class JobRunner implements Runnable {
    private JobControl control;

    public JobRunner(JobControl control) {
      this.control = control;
    }

    public void run() {
      this.control.run();
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("Usage: tfidf <in> <out>");
      return 2;
    }
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    Path userInputPath = new Path(args[0]);
    Path userOutputPath = new Path(args[1]);
    if (fs.exists(userOutputPath)) {
      fs.delete(userOutputPath, true);
    }
    Path wordFreqPath = new Path(OUTPUT_PATH);
    if (fs.exists(wordFreqPath)) {
      fs.delete(wordFreqPath, true);
    }
    Path wordCountsPath = new Path(OUTPUT_PATH_2);
    if (fs.exists(wordCountsPath)) {
      fs.delete(wordCountsPath, true);
    }

    FileStatus[] userFiles = fs.listStatus(userInputPath);
    final int numberOfUserInputFiles = userFiles.length;
    String[] fileNames = new String[numberOfUserInputFiles];
    for (int i = 0; i < numberOfUserInputFiles; i++) {
      fileNames[i] = userFiles[i].getPath().getName();
    }

    Job job = new Job(conf, "Word Frequency In Document");
    job.setJarByClass(WordFrequencyInDocument.class);
    job.setMapperClass(WordFrequencyInDocument.WordFrequencyInDocMapper.class);
    job.setReducerClass(WordFrequencyInDocument.WordFrequencyInDocReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, userInputPath);
    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

    ControlledJob controlledJob1 = new ControlledJob(job, null);

    Configuration conf2 = getConf();
    conf2.setStrings("documentsInCorpusList", fileNames);
    Job job2 = new Job(conf2, "Words Counts");
    job2.setJarByClass(WordCountsInDocuments.class);
    job2.setMapperClass(WordCountsInDocuments.WordCountsForDocsMapper.class);
    job2.setReducerClass(WordCountsInDocuments.WordCountsForDocsReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
    FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH_2));

    ControlledJob controlledJob2 = new ControlledJob(job2, null);
    controlledJob2.addDependingJob(controlledJob1);

    Configuration conf3 = getConf();
    conf3.setInt("numberOfDocsInCorpus", numberOfUserInputFiles);
    Job job3 = new Job(conf3, "TF-IDF of Words in Corpus");
    job3.setJarByClass(WordsInCorpusTFIDF.class);
    job3.setMapperClass(WordsInCorpusTFIDF.WordsInCorpusTFIDFMapper.class);
    job3.setReducerClass(WordsInCorpusTFIDF.WordsInCorpusTFIDFReducer.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job3, new Path(OUTPUT_PATH_2));
    FileOutputFormat.setOutputPath(job3, userOutputPath);

    ControlledJob controlledJob3 = new ControlledJob(job3, null);
    controlledJob3.addDependingJob(controlledJob2);

    JobControl control = new JobControl("TF-IDF");
    control.addJob(controlledJob1);
    control.addJob(controlledJob2);
    control.addJob(controlledJob3);

    JobRunner runner = new JobRunner(control);
    new Thread(runner).start();
    while (!control.allFinished()) {
      System.out.println("Still running...");
      Thread.sleep(5000);
    }
    return control.getFailedJobList().size() == 0 ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TFIDFJobController(), args);
    System.exit(res);
  }
}
