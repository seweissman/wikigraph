package wikigraph;


import java.io.IOException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfStringLong;
import edu.umd.cloud9.io.pair.PairOfStrings;
	
	public class CombineUserEditGraph extends Configured implements Tool {
	    private static final Logger LOG = Logger.getLogger(CombineUserEditGraph.class);

 
	    
	    private static class EditGraphReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
			
			@Override
			public void reduce(PairOfStrings edge, Iterable<IntWritable> counts, Context context)
			       throws IOException, InterruptedException {
				//System.out.println("key = " + key);
				int total = 0;

				Iterator<IntWritable> countIt = counts.iterator();
				while(countIt.hasNext()){
					IntWritable c = countIt.next();
					total += c.get();
				}

				if(total > 1){
					IntWritable countOut = new IntWritable();
					countOut.set(total);
					context.write(edge, countOut);
				}
				
			}
			

	    }


	    
	 
	    private static final String INPUT = "input";
	    private static final String OUTPUT = "output";
	    	    
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
	        String outputPath = cmdline.getOptionValue(OUTPUT);

	        

	        LOG.info("Tool name: " + this.getClass().getName());
	        LOG.info(" - input file: " + inputPath);
	        LOG.info(" - output file: " + outputPath);

	        Configuration conf = getConf();
	        // Set heap space - using old API
	        conf.set("mapred.job.map.memory.mb", "2048");
	        conf.set("mapred.map.child.java.opts", "-Xmx2048m");
	        conf.set("mapred.job.reduce.memory.mb", "6144");
	        conf.set("mapred.reduce.child.java.opts", "-Xmx6144m");
	        conf.set("xmlinput.start","page");
	        conf.set("xmlinput.end","page");
	        //conf.set("mapred.child.java.opts", "-Xmx2048m");

	        Job job = Job.getInstance(conf);
	        //JobConf conf = new JobConf(getConf(), BuildUserProfile.class);
	        job.setJobName(String.format("BuildUserEditGraph[%s: %s, %s: %s]", INPUT, inputPath, OUTPUT, outputPath));
	               

	        job.setNumReduceTasks(20);

	       // job.setMapperClass(EditGraphMapper.class);
	        job.setReducerClass(EditGraphReducer.class);
	        job.setCombinerClass(EditGraphReducer.class);
	        //job.setPartitionerClass(UserPartitioner.class);
	        
	        //conf.setInputFormat(WikipediaPageInputFormat.class);
	        job.setInputFormatClass(XMLInputFormat.class);
	        job.setOutputFormatClass(SequenceFileOutputFormat.class);
	        //conf.setOutputFormat(TextOutputFormat.class);
	        
	        
	        job.setOutputKeyClass(PairOfStrings.class);
	        job.setOutputValueClass(IntWritable.class);
	        
	        FileSystem fs = FileSystem.get(conf);        
	        Path outPath = new Path(outputPath);
	        Path inPath = new Path(inputPath);
	        
	        FileStatus[] inglob = fs.globStatus(inPath);
	        for(FileStatus f : inglob){
	        	FileInputFormat.addInputPath(job, f.getPath());
	        }
	        //FileInputFormat.setInputPaths(job, new Path(inputPath));
	        FileOutputFormat.setOutputPath(job, outPath);
	        
	        // Delete the output directory if it exists already.
	        fs.delete(outPath, true);

	        long startTime = System.currentTimeMillis();
	        job.waitForCompletion(true);
	        LOG.info("Total Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
	        
	        return 0;
	    }
	    public CombineUserEditGraph() {}

	    public static void main(String[] args) throws Exception {
	        ToolRunner.run(new CombineUserEditGraph(), args);
	    }
	}


