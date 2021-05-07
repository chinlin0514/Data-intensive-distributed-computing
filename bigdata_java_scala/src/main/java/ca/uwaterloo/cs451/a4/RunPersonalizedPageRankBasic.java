// /**
//  * Bespin: reference implementations of "big data" algorithms
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  * http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */

package ca.uwaterloo.cs451.a4;

import io.bespin.java.mapreduce.pagerank.NonSplitableSequenceFileInputFormat;
import io.bespin.java.mapreduce.pagerank.PageRankNode;
import io.bespin.java.mapreduce.pagerank.RangePartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.map.HMapIF;
import tl.lin.data.map.MapIF;
import io.bespin.java.mapreduce.pagerank.NonSplitableSequenceFileInputFormat;
import io.bespin.java.mapreduce.pagerank.PageRankNode;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tl.lin.data.array.ArrayListOfIntsWritable;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class RunPersonalizedPageRankBasic extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RunPersonalizedPageRankBasic.class);


  private static enum PageRank {
    nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
  };

  // Mapper, no in-mapper combining.
  private static class MapC extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

    // The neighbor to which we're sending messages.
    private static final IntWritable neighbor = new IntWritable();

    // Contents of the messages: partial PageRank mass.
    private static final PageRankNode intermediateMass = new PageRankNode();

    // For passing along node structure.
    private static final PageRankNode intermediateStructure = new PageRankNode();

    private static final ArrayList<Integer> sources = new ArrayList<Integer>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      String[] nodeSources = context.getConfiguration().getStrings("NODE_SRC", "");
      for (String src : nodeSources) {
        sources.add(Integer.valueOf(src));
      }
    }
    @Override
    protected void map(IntWritable nodeID, PageRankNode node, Context context)
            throws IOException, InterruptedException {

        intermediateStructure.setNodeId(node.getNodeId());
        intermediateStructure.setType(PageRankNode.Type.Structure);
        intermediateStructure.setAdjacencyList(node.getAdjacencyList());

        context.write(nodeID, intermediateStructure);

        int massMessages = 0;

        if (node.getAdjacencyList().size() > 0) {
            ArrayListOfIntsWritable list = node.getAdjacencyList();
            float mass = node.getPageRank() - (float) StrictMath.log((float) (list.size()));

            context.getCounter(PageRank.edges).increment(list.size());

            for (int i = 0; i < list.size(); ++i) {
                neighbor.set(list.get(i));
                intermediateMass.setNodeId(list.get(i));
                intermediateMass.setType(PageRankNode.Type.Mass);
                intermediateMass.setPageRank(mass);
                context.write(neighbor, intermediateMass);
                massMessages++;
            }
        } else {
            float mass =  node.getPageRank() - (float) StrictMath.log((float) (sources.size()));

            for (Integer sourceID: sources) {
                neighbor.set(sourceID); 
                intermediateMass.setNodeId(sourceID);
                intermediateMass.setType(PageRankNode.Type.Mass);
                intermediateMass.setPageRank(mass);
                context.write(neighbor, intermediateMass);
                massMessages++;
            }
        }
        context.getCounter(PageRank.nodes).increment(1);
        context.getCounter(PageRank.massMessages).increment(massMessages);
    }

    @Override
    public void cleanup(Context context) throws IOException {
      sources.clear();
    }
  }

  // Combiner: sums partial PageRank contributions and passes node structure along.
  private static class CombineC extends
      Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {

    private static final PageRankNode intermediateMass = new PageRankNode();

    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> values, Context context)
        throws IOException, InterruptedException {
      int massMessages = 0;

      // Remember, PageRank mass is stored as a log prob.
      float mass = Float.NEGATIVE_INFINITY;
      for (PageRankNode n : values) {
        if (n.getType() == PageRankNode.Type.Structure) {
          // Simply pass along node structure.
          context.write(nid, n);
        } else {
          // Accumulate PageRank mass contributions.
          mass = sumLogProbs(mass, n.getPageRank());
          massMessages++;
        }
      }

      // Emit aggregated results.
      if (massMessages > 0) {
        intermediateMass.setNodeId(nid.get());
        intermediateMass.setType(PageRankNode.Type.Mass);
        intermediateMass.setPageRank(mass);

        context.write(nid, intermediateMass);
      }
    }
  }

  // Reduce: sums incoming PageRank contributions, rewrite graph structure.
  private static class ReduceC extends
      Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    
    private static final ArrayList<Integer> sources = new ArrayList<>();
    private static float ALPHA;
    private static float SOURCE_NODE_COUNT;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      String[] nodeSources = context.getConfiguration().getStrings("NODE_SRC", "");
      for (String src : nodeSources) {
        sources.add(Integer.valueOf(src));
      }
      SOURCE_NODE_COUNT = (float) sources.size();
      ALPHA = context.getConfiguration().getFloat("ALPHA", 0.15f);
    }

    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> iterable, Context context)
        throws IOException, InterruptedException {
      Iterator<PageRankNode> values = iterable.iterator();

      // Create the node structure that we're going to assemble back together from shuffled pieces.
      PageRankNode node = new PageRankNode();

      node.setType(PageRankNode.Type.Complete);
      node.setNodeId(nid.get());

      int massMessagesReceived = 0;
      int structureReceived = 0;

      float mass = Float.NEGATIVE_INFINITY;
      while (values.hasNext()) {
        PageRankNode n = values.next();

        if (n.getType().equals(PageRankNode.Type.Structure)) {
          // This is the structure; update accordingly.
          ArrayListOfIntsWritable list = n.getAdjacencyList();
          structureReceived++;

          node.setAdjacencyList(list);
        } else {
          // This is a message that contains PageRank mass; accumulate.
          mass = sumLogProbs(mass, n.getPageRank());
          massMessagesReceived++;
        }
      }

      mass = (float) (StrictMath.log(1.0f - ALPHA)) + mass;

      if (sources.contains(nid.get())) {
          // Handle Source Node Case
          mass = sumLogProbs(mass,
                  (float) (StrictMath.log(ALPHA) - StrictMath.log(SOURCE_NODE_COUNT)));
      }
      // Update the final accumulated PageRank mass.
      node.setPageRank(mass);
      context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

      // Error checking.
      if (structureReceived == 1) {
        // Everything checks out, emit final node structure with updated PageRank value.
        context.write(nid, node);
        // Keep track of total PageRank mass.
      } else if (structureReceived == 0) {
        // We get into this situation if there exists an edge pointing to a node which has no
        // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
        // log and count but move on.
        context.getCounter(PageRank.missingStructure).increment(1);
        LOG.warn("No structure received for nodeid: " + nid.get() + " mass: "
            + massMessagesReceived);
        // It's important to note that we don't add the PageRank mass to total... if PageRank mass
        // was sent to a non-existent node, it should simply vanish.
      } else {
        // This shouldn't happen!
        throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
            + " mass: " + massMessagesReceived + " struct: " + structureReceived);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      sources.clear();
    }
  }

  public RunPersonalizedPageRankBasic() {}

  private static final NumberFormat formatter = new DecimalFormat("0000");
  private static final String BASE = "base";
  private static final String NUM_NODES = "numNodes";
  private static final String START = "start";
  private static final String END = "end";
  private static final String SOURCES = "sources";
  private static final String USE_COMBINER = "useCombiner";
  private static final String ALPHA_VALUE = "alpha";

  /**
   * Runs this tool.
   */

  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(new Option(USE_COMBINER, "Combiner"));
    options.addOption(new Option(ALPHA_VALUE, "alpha: Teleporting "));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("base path").create(BASE));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("start iteration").create(START));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("end iteration").create(END));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of nodes").create(NUM_NODES));
    options.addOption(OptionBuilder.withArgName("sources").hasArg()
        .withDescription("source nodes").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START) ||
        !cmdline.hasOption(END) || !cmdline.hasOption(NUM_NODES) ||
        !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String basePath = cmdline.getOptionValue(BASE);
    int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
    int s = Integer.parseInt(cmdline.getOptionValue(START));
    int e = Integer.parseInt(cmdline.getOptionValue(END));
    boolean useCombiner = cmdline.hasOption(USE_COMBINER);
    float alpha = Float.parseFloat(cmdline.getOptionValue(ALPHA_VALUE, "0.15"));
    String sources = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: RunPageRank");
    LOG.info(" - base path: " + basePath);
    LOG.info(" - num nodes: " + n);
    LOG.info(" - start iteration: " + s);
    LOG.info(" - end iteration: " + e);
    LOG.info(" - use combiner: " + useCombiner);
    LOG.info(" - Alpha value: " + alpha);
    LOG.info(" - sources: " + sources);

    // Iterate pageRanking.
    for (int i = s; i < e; i++) {
      iteratePageRank(i, i + 1, basePath, n, useCombiner, alpha, sources);
    }

    return 0;
  }

  // Run each iteration.
  private void iteratePageRank(int i, int j, String basePath, int numNodes, boolean useCombiner, float alpha, String sources) 
    throws Exception {

      Job job = Job.getInstance(getConf());
      job.setJobName(RunPersonalizedPageRankBasic.class.getSimpleName() + " Iteration: " + i);
      job.setJarByClass(RunPersonalizedPageRankBasic.class);

      String in = basePath + "/iter" + formatter.format(i);
      String out = basePath + "/iter" + formatter.format(j);

      int numPartitions = 0;
      for (FileStatus file : FileSystem.get(getConf()).listStatus(new Path(in))) {
          if (file.getPath().getName().contains("part-"))
              numPartitions++;
      }

      LOG.info(RunPersonalizedPageRankBasic.class.getSimpleName() + " iteration: " + i);
      LOG.info("input path: " + in);
      LOG.info("output path: " + out);
      LOG.info("total nodes: " + numNodes);
      LOG.info("use combiner: " + useCombiner);
      LOG.info("Alpha: " + alpha);
      LOG.info("Source: " + sources);
      LOG.info("number of partitions: " + numPartitions);

      int numReducers = numPartitions;
      job.getConfiguration().setInt("NodeCount", numNodes);
      job.getConfiguration().setBoolean("mapreduce.map.speculative", false);
      job.getConfiguration().setBoolean("mapreduce.reduce.speculative", false);
      job.getConfiguration().setFloat("ALPHA", alpha);
      job.getConfiguration().setStrings("NODE_SRC", sources);

      job.setNumReduceTasks(numReducers);

      FileInputFormat.setInputPaths(job, new Path(in));
      FileOutputFormat.setOutputPath(job, new Path(out));

      job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);

      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(PageRankNode.class);

      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(PageRankNode.class);

      job.setMapperClass(MapC.class);
      job.setCombinerClass(CombineC.class);
      job.setReducerClass(ReduceC.class);
      FileSystem.get(getConf()).delete(new Path(out), true);

      long startTime = System.currentTimeMillis();
      job.waitForCompletion(true);
      System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
  }

  private static float sumLogProbs(float a, float b) {
      if (a == Float.NEGATIVE_INFINITY)
          return b;

      if (b == Float.NEGATIVE_INFINITY)
          return a;

      if (a < b) {
          return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
      }

      return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
  }

  public static void main(String[] args) throws Exception {
      ToolRunner.run(new RunPersonalizedPageRankBasic(), args);
  }
}

    
