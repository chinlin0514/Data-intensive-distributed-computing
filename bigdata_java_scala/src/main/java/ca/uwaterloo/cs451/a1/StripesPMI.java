package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.map.HMapStIW;
import tl.lin.data.map.HashMapWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Set;

/**
 * 
 * Stripes PMI 
 * @author ChinLin Chen
 * 
 */

public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);
  private static final int WORD_LIMIT = 40;

  public static final class MapperCount extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private static final Text KEY = new Text();
    public enum MyCounter {LINE_COUNTER}; // Counters

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      int number = 0;
      context.getCounter(MyCounter.LINE_COUNTER).increment(1);
      Set<String> wordset = new HashSet<String>();
      for (String word : Tokenizer.tokenize(value.toString())) {
        wordset.add(word);
        number++;
        if (number >= WORD_LIMIT) break;
      }
      Iterator<String> it = wordset.iterator();
      while(it.hasNext()){
        KEY.set(it.next());
        context.write(KEY,ONE);
      }
    }
  }

  public static final class ReducerCount extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      SUM.set(sum);
      context.write(key, SUM);
    }
  }
  // Second job 
  // Mapper from ComputeCooccurrenceMatrixStripes.java
  public static final class MapperPMI extends Mapper<LongWritable, Text, Text, HMapStIW> {
    // use HMapStIW in stripes 
    private static final HMapStIW MAP = new HMapStIW();
    private static final Text KEY = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      int numWords = 0;
      Set<String> set = new HashSet<String>();
      for (String word : Tokenizer.tokenize(value.toString())) {
        set.add(word);
        numWords++;
        if (numWords >= WORD_LIMIT) break;
      }

      String[] words = new String[set.size()];
      words = set.toArray(words);
      int len = words.length;
      for (int i = 0; i < len; i++) {
        MAP.clear();
        for (int j = 0; j < len; j++) {
          if (i == j) continue;
          MAP.increment(words[j]);
        }
        KEY.set(words[i]);
        context.write(KEY, MAP);
      }
    }
  }

  // Combiner from ComputeCooccurrenceMatrixStripes.java
  public static final class CombinerPMI extends Reducer<Text, HMapStIW, Text, HMapStIW> {
    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }

  public static final class ReducerPMI extends Reducer<Text, HMapStIW, Text, HashMapWritable> {
    private static final Text KEY = new Text();
    private static final HashMapWritable MAP = new HashMapWritable();
    private static final Map<String, Integer> wordCount = new HashMap<String, Integer>();
    private static long lineCount;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      lineCount = conf.getLong("counter", 0L);

      FileSystem fs = FileSystem.get(conf);
      FileStatus[] status = fs.globStatus(new Path("tmp/part-r-*"));
      try{
        for (FileStatus file : status) {
          FSDataInputStream is = fs.open(file.getPath());
          InputStreamReader isr = new InputStreamReader(is, "UTF-8");
          BufferedReader br = new BufferedReader(isr);
          String line = br.readLine();
          while (line != null) {
            String[] data = line.split("\\s+");
            if (data.length == 2) {
              wordCount.put(data[0], Integer.parseInt(data[1]));
            }
            line = br.readLine();
          }
          br.close();
        }
      } catch (Exception e){
        System.out.println("Something went wrong.");
      }
      }

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      Configuration conf = context.getConfiguration();
      int threshold = conf.getInt("threshold", 0);

      String left = key.toString();
      KEY.set(left);
      MAP.clear();
      for (String right : map.keySet()) {
        if (map.get(right) >= threshold) {
          int sum = map.get(right);

          double rightVal = wordCount.get(right);
          double leftVal = wordCount.get(left); 
          double denom = rightVal * leftVal; 
          double numer = sum * lineCount; 
          float pmi = (float) Math.log10(numer/denom); 
          PairOfFloatInt PMI = new PairOfFloatInt();
          PMI.set(pmi, sum);
          MAP.put(right, PMI);
        }
      }
      if(!MAP.isEmpty()){
        context.write(KEY,MAP);
      }
    }
  }


  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-threshold", metaVar = "[num]", usage = "threshold of co-occurrence")
    int threshold = 1;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    String sideDataPath = "tmp/";

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);
    LOG.info(" - threshold: " + args.threshold);

    Configuration conf = getConf();
    conf.set("sideDataPath", sideDataPath);
    conf.set("threshold", Integer.toString(args.threshold));
    Job job = Job.getInstance(conf);
    job.setJobName(StripesPMI.class.getSimpleName());
    job.setJarByClass(StripesPMI.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(sideDataPath));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MapperCount.class);
    job.setCombinerClass(ReducerCount.class);
    job.setReducerClass(ReducerCount.class);

    job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    // Delete the output directory if it exists already.
    Path outputDir = new Path(sideDataPath);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


    // Second Job
    long count = job.getCounters().findCounter(MapperCount.MyCounter.LINE_COUNTER).getValue();
    conf.setLong("counter", count);
    Job secondJob = Job.getInstance(conf);
    secondJob.setJobName(StripesPMI.class.getSimpleName());
    secondJob.setJarByClass(StripesPMI.class);

    secondJob.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(secondJob, new Path(args.input));
    FileOutputFormat.setOutputPath(secondJob, new Path(args.output));

    secondJob.setMapOutputKeyClass(Text.class);
    secondJob.setMapOutputValueClass(HMapStIW.class);
    secondJob.setOutputKeyClass(Text.class);
    secondJob.setOutputValueClass(HashMapWritable.class);
    secondJob.setOutputFormatClass(TextOutputFormat.class);

    secondJob.setMapperClass(MapperPMI.class);
    secondJob.setCombinerClass(CombinerPMI.class);
    secondJob.setReducerClass(ReducerPMI.class);

    secondJob.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    secondJob.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    secondJob.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    secondJob.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    secondJob.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    // Delete the output directory if it exists already.
    outputDir = new Path(args.output);
    FileSystem.get(conf).delete(outputDir, true);

    startTime = System.currentTimeMillis();
    secondJob.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}
