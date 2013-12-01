package org.fhmuenster.bde.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * <p>Einfaches WordCount MapReduce Programm. Hier wird exemplarisch <i>nicht</i> die
 * Hadoop <code>Tool</code>, <code>ToolRunner</code> und <code>Configured</code>
 * Klassen benutzt, sondern der <code>GenericOptionsParser</code> direkt.</p>
 *
 * <p>Die Mapper und Reducer Klassen sind ausserdem in externen Klassen angelegt. Diese
 * könnten auch hier als innere Klassen definiert werden. Beide Wege sind gleichwertig
 * und können nach eigenem Ermessen ausgewählt werden.</p>
 */
public class WordCount {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf,
      args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizingMapper.class);
    job.setCombinerClass(SummingReducer.class);
    job.setReducerClass(SummingReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
