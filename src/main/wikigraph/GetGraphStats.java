package wikigraph;


import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfStrings;

	
	public class GetGraphStats extends Configured implements Tool {
	    private static final Logger LOG = Logger.getLogger(GetGraphStats.class);

	    private static class WeightDistributionMapper extends Mapper<PairOfStrings, IntWritable, IntWritable, IntWritable> {

			public static final IntWritable ONE = new IntWritable(1);
			@Override
			public void map(PairOfStrings edge, IntWritable weight, Context context)
			       throws IOException, InterruptedException {
				context.write(weight,ONE);
			}
			
	    }

	    private static class WeightDistributionReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	    	
			@Override
			public void reduce(IntWritable weight, Iterable<IntWritable> counts, Context context)
			       throws IOException, InterruptedException {
				IntWritable countOut = new IntWritable();
				Iterator<IntWritable> countIt = counts.iterator();
				int sumcount = 0;
				while(countIt.hasNext()){
					sumcount += countIt.next().get();
				}
				countOut.set(sumcount);
				context.write(weight, countOut);
			}
	    }
	    
	    private static class WeightedDegreeCentralityMapper extends Mapper<PairOfStrings, IntWritable, Text, PairOfInts> {

			
			@Override
			public void map(PairOfStrings edge, IntWritable weight, Context context)
			       throws IOException, InterruptedException {
				Text fromNode = new Text();
				Text toNode = new Text();
				PairOfInts fromInOutDegree = new PairOfInts();
				PairOfInts toInOutDegree = new PairOfInts();
				fromNode.set(edge.getLeftElement());
				fromInOutDegree.set(0, weight.get());
				toNode.set(edge.getLeftElement());
				toInOutDegree.set(weight.get(), 0);
				
				context.write(fromNode,fromInOutDegree);
				context.write(toNode,toInOutDegree);
			}
			
	    }

	    private static class WeightedDegreeCentralityReducer extends Reducer<Text, PairOfInts, Text, PairOfInts> {
	    	
			@Override
			public void reduce(Text user, Iterable<PairOfInts> inoutdegrees, Context context)
			       throws IOException, InterruptedException {
				PairOfInts ioOut = new PairOfInts();
				int indegreesum = 0;
				int outdegreesum = 0;
				Iterator<PairOfInts> ioIt = inoutdegrees.iterator();
				while(ioIt.hasNext()){
					PairOfInts io = ioIt.next();
					indegreesum += io.getLeftElement();
					outdegreesum += io.getRightElement();
				}
				ioOut.set(indegreesum, outdegreesum);
				context.write(user, ioOut);
			}
	    }
	    
	    private static class DegreeCentralityMapper extends Mapper<PairOfStrings, IntWritable, Text, PairOfInts> {

			
			@Override
			public void map(PairOfStrings edge, IntWritable weight, Context context)
			       throws IOException, InterruptedException {
				Text fromNode = new Text();
				Text toNode = new Text();
				PairOfInts fromInOutDegree = new PairOfInts();
				PairOfInts toInOutDegree = new PairOfInts();
				fromNode.set(edge.getLeftElement());
				fromInOutDegree.set(0, 1);
				toNode.set(edge.getLeftElement());
				toInOutDegree.set(1, 0);
				
				context.write(fromNode,fromInOutDegree);
				context.write(toNode,toInOutDegree);
			}
			
	    }

	    private static class DegreeCentralityReducer extends Reducer<Text, PairOfInts, Text, PairOfInts> {
	    	
			@Override
			public void reduce(Text user, Iterable<PairOfInts> inoutdegrees, Context context)
			       throws IOException, InterruptedException {
				PairOfInts ioOut = new PairOfInts();
				int indegreesum = 0;
				int outdegreesum = 0;
				Iterator<PairOfInts> ioIt = inoutdegrees.iterator();
				while(ioIt.hasNext()){
					PairOfInts io = ioIt.next();
					indegreesum += io.getLeftElement();
					outdegreesum += io.getRightElement();
				}
				ioOut.set(indegreesum, outdegreesum);
				context.write(user, ioOut);
			}
	    }
	    
	    
	    
	    private static final String INPUT = "input";
	    private static final String OUTPUT = "outprefix";
	    	    
	    @SuppressWarnings("static-access")
	    @Override
	    public int run(String[] args) throws Exception {
	        Options options = new Options();
	        options.addOption(OptionBuilder.withArgName("path")
	                .hasArg().withDescription("bz2 input path").create(INPUT));
	        options.addOption(OptionBuilder.withArgName("path")
	                .hasArg().withDescription("output path").create(OUTPUT));
	        
	        CommandLine cmdline;
	        CommandLineParser parser = new GnuParser();
	        try {
	            cmdline = parser.parse(options, args);
	        } catch (ParseException exp) {
	            System.err.println("Error parsing command line: " + exp.getMessage());
	            return -1;
	        }

	        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)){ 
	            HelpFormatter formatter = new HelpFormatter();
	            formatter.setWidth(120);
	            formatter.printHelp(this.getClass().getName(), options);
	            ToolRunner.printGenericCommandUsage(System.out);
	            return -1;
	        }

	        String inputPath = cmdline.getOptionValue(INPUT);
	        String outputPrefix = cmdline.getOptionValue(OUTPUT);

	        

	        LOG.info("Tool name: " + this.getClass().getName());
	        LOG.info(" - input file: " + inputPath);
	        LOG.info(" - output prefix: " + outputPrefix);

	        Configuration conf = getConf();
	        // Set heap space - using old API
	        conf.set("mapred.job.map.memory.mb", "2048");
	        conf.set("mapred.map.child.java.opts", "-Xmx2048m");
	        conf.set("mapred.job.reduce.memory.mb", "6144");
	        conf.set("mapred.reduce.child.java.opts", "-Xmx6144m");
	        conf.set("xmlinput.start","page");
	        conf.set("xmlinput.end","page");
	        //conf.set("mapred.child.java.opts", "-Xmx2048m");

	        String outputPath;	        
	        // Job 1
	        Job job = Job.getInstance(conf);
	        //JobConf conf = new JobConf(getConf(), BuildUserProfile.class);
	        outputPath = outputPrefix + ".weights";
	        job.setJobName(String.format("WeightDistributsion[%s: %s, output: %s]", INPUT, inputPath, outputPath));
	               

	        job.setNumReduceTasks(4);

	        job.setMapperClass(WeightDistributionMapper.class);
	        job.setReducerClass(WeightDistributionReducer.class);
	        job.setCombinerClass(WeightDistributionReducer.class);
	        
	        job.setInputFormatClass(SequenceFileInputFormat.class);
	        job.setOutputFormatClass(SequenceFileOutputFormat.class);
	        //conf.setOutputFormat(TextOutputFormat.class);
	        
	        
	        job.setOutputKeyClass(IntWritable.class);
	        job.setOutputValueClass(IntWritable.class);
	        
	        FileSystem fs = FileSystem.get(conf);        
	        Path outPath = new Path(outputPath);
	        Path inPath = new Path(inputPath);
	        
	        FileInputFormat.setInputPaths(job, inPath);
	        FileOutputFormat.setOutputPath(job, outPath);
	        
	        // Delete the output directory if it exists already.
	        fs.delete(outPath, true);

	        long startTime = System.currentTimeMillis();
	        job.waitForCompletion(true);
	        LOG.info("Total Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

	        // Job 2
	        job = Job.getInstance(conf);
	        //JobConf conf = new JobConf(getConf(), BuildUserProfile.class);
	        outputPath = outputPrefix + ".wdegree";
	        job.setJobName(String.format("WeightedDegreeCentrality[%s: %s, output: %s]", INPUT, inputPath, outputPath));

	        job.setNumReduceTasks(4);

	        job.setMapperClass(WeightedDegreeCentralityMapper.class);
	        job.setReducerClass(WeightedDegreeCentralityReducer.class);
	        job.setCombinerClass(WeightedDegreeCentralityReducer.class);
	        
	        job.setInputFormatClass(SequenceFileInputFormat.class);
	        job.setOutputFormatClass(SequenceFileOutputFormat.class);
	        
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(PairOfInts.class);
	        
	        outPath = new Path(outputPath);
	        inPath = new Path(inputPath);
	        
	        FileInputFormat.setInputPaths(job, inPath);
	        FileOutputFormat.setOutputPath(job, outPath);
	        
	        // Delete the output directory if it exists already.
	        fs.delete(outPath, true);

	        startTime = System.currentTimeMillis();
	        job.waitForCompletion(true);
	        LOG.info("Total Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

	        // Job 3
	        job = Job.getInstance(conf);
	        outputPath = outputPrefix + ".degree";
	        //JobConf conf = new JobConf(getConf(), BuildUserProfile.class);
	        job.setJobName(String.format("DegreeCentrality[%s: %s, output: %s]", INPUT, inputPath, outputPath));
	               

	        job.setNumReduceTasks(4);

	        job.setMapperClass(DegreeCentralityMapper.class);
	        job.setReducerClass(DegreeCentralityReducer.class);
	        job.setCombinerClass(DegreeCentralityReducer.class);
	        
	        job.setInputFormatClass(SequenceFileInputFormat.class);
	        job.setOutputFormatClass(SequenceFileOutputFormat.class);
	        //conf.setOutputFormat(TextOutputFormat.class);
	        
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(PairOfInts.class);
	        
	        outPath = new Path(outputPath);
	        inPath = new Path(inputPath);
	        
	        FileInputFormat.setInputPaths(job, inPath);
	        FileOutputFormat.setOutputPath(job, outPath);
	        
	        // Delete the output directory if it exists already.
	        fs.delete(outPath, true);

	        startTime = System.currentTimeMillis();
	        job.waitForCompletion(true);
	        LOG.info("Total Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

	        return 0;
	    }

	    public GetGraphStats() {}

	    public static void main(String[] args) throws Exception {
	        ToolRunner.run(new GetGraphStats(), args);
	    }
	}


