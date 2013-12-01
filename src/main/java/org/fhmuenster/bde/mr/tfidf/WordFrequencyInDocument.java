package org.fhmuenster.bde.mr.tfidf;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordFrequencyInDocument extends Configured implements Tool {

  private static final String OUTPUT_PATH = "1-word-freq";

  public static class WordFrequencyInDocMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final Pattern PATTERN = Pattern.compile("\\w+");

    private static Set<String> googleStopwords;
    static {
      googleStopwords = new HashSet<String>();
      googleStopwords.add("I");
      googleStopwords.add("a");
      googleStopwords.add("about");
      googleStopwords.add("an");
      googleStopwords.add("are");
      googleStopwords.add("as");
      googleStopwords.add("at");
      googleStopwords.add("be");
      googleStopwords.add("by");
      googleStopwords.add("com");
      googleStopwords.add("de");
      googleStopwords.add("en");
      googleStopwords.add("for");
      googleStopwords.add("from");
      googleStopwords.add("how");
      googleStopwords.add("in");
      googleStopwords.add("is");
      googleStopwords.add("it");
      googleStopwords.add("la");
      googleStopwords.add("of");
      googleStopwords.add("on");
      googleStopwords.add("or");
      googleStopwords.add("that");
      googleStopwords.add("the");
      googleStopwords.add("this");
      googleStopwords.add("to");
      googleStopwords.add("was");
      googleStopwords.add("what");
      googleStopwords.add("when");
      googleStopwords.add("where");
      googleStopwords.add("who");
      googleStopwords.add("will");
      googleStopwords.add("with");
      googleStopwords.add("and");
      googleStopwords.add("the");
      googleStopwords.add("www");
    }

    private Text word = new Text();
    private IntWritable ONE = new IntWritable(1);

    public void map(LongWritable offset, Text line,
      Context context) throws IOException, InterruptedException {
      Matcher m = PATTERN.matcher(line.toString());
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
      StringBuilder value = new StringBuilder();
      while (m.find()) {
        String matchedKey = m.group().toLowerCase();
        if (!Character.isLetter(matchedKey.charAt(0)) ||
          Character.isDigit(matchedKey.charAt(0)) ||
          googleStopwords.contains(matchedKey) || matchedKey.contains("_") ||
          matchedKey.length() < 3) {
          continue;
        }
        value.append(matchedKey);
        value.append("@");
        value.append(fileName);
        word.set(value.toString());
        context.write(word, ONE);
        value.setLength(0);
      }
    }
  }

  public static class WordFrequencyInDocReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable wordSum = new IntWritable();

    protected void reduce(Text key, Iterable<IntWritable> values,
      Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      this.wordSum.set(sum);
      context.write(key, this.wordSum);
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println("Usage: tfidf <in>");
      return 2;
    }
    Configuration conf = getConf();
    Job job = new Job(conf, "Word Frequency In Document");
    job.setJarByClass(WordFrequencyInDocument.class);
    job.setMapperClass(WordFrequencyInDocMapper.class);
    job.setReducerClass(WordFrequencyInDocReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(),
      new WordFrequencyInDocument(), args);
    System.exit(res);
  }

}