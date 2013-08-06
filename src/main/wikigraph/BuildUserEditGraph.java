package wikigraph;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.XMLInputFormatOld;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfLongs;
import edu.umd.cloud9.io.pair.PairOfStrings;
	
	public class BuildUserEditGraph extends Configured implements Tool {
	    private static final Logger LOG = Logger.getLogger(BuildUserEditGraph.class);

	    /* SignatureeMapper
	     * 
	     * Parameters that can be tweaked: NHASH, NHASHOUTPUTBITS, MINLEN
	     * 
	     * Pulls out sentences from text input using a regex. 
	     * Emits one NHASH-length minhash signature per sentence.
	     * Each hash is NHASHOUTPUTBITS long. (So signature is NHASH*NHASHOUTPUTBITS long.)
	     * Sentences are shingled by individual words. 
	     * If sentences are less than MINLEN words, then they are skipped.
	     * 
	     * 
	     * Output values are (offset,nsentence) where offset is the byte offset of the input line in the
	     * input text and nsentence is the number of the sentence in the line. (starting from 0)
	     * 
	     */

	    
	    private static class LanguageMapper extends MapReduceBase implements
	    Mapper<LongWritable, Text, Text, PairOfStrings> {
	        
	    	static final Pattern titlePattern = Pattern.compile(".*<title>(.*)<\\/title>.*");
	    	static final Pattern beginRevisionPattern = Pattern.compile(".*<revision>.*");
	    	static final Pattern endRevisionPattern = Pattern.compile(".*<\\/revision>.*");
	    	static final Pattern userNamePattern = Pattern.compile(".*<username>(.*)<\\/username>.*");
	    	static final Pattern ipPattern = Pattern.compile(".*<ip>(.*)<\\/ip>.*");

	           public void map(LongWritable key, Text p, OutputCollector<Text, PairOfStrings> output,
	                    Reporter reporter) throws IOException {
	               
	               // users & articles with weights?
	        	   // users to users consecutive edits with weights
	        	   // some sort of user editing diagram
	        	   // time to next edit
	        	   // highly protected/controversial articles
	            String lines[] = p.toString().split("\n");
	            System.out.println("key = " + key);
	            Matcher m;
	            String title = null;
	            String user = null;
	            String lastUser = null;
	            String ip = null;
	            Text titleOut = null;
	            PairOfStrings userEdge;
	            for(String line: lines){
					System.out.println("LINE " + line);
	            	m = titlePattern.matcher(line);
	            	if((m = endRevisionPattern.matcher(line)).matches()){
						System.out.println("\tend revision\n");
						System.out.println("user = " + ((user != null)?user:"NULL"));
						System.out.println("lastUser = " + ((lastUser != null)?lastUser:"NULL"));
						System.out.println("ip = " + ((ip != null)?ip:"NULL"));
						
	            		if(lastUser != null	&& titleOut != null && (user != null || ip != null)){
	            			userEdge = new PairOfStrings();
	            			if(user != null) userEdge.set(user, lastUser);
	            			if(ip != null) userEdge.set(ip, lastUser);
	            			output.collect(titleOut, userEdge);
						}
	            		lastUser = user;
	            		user = null;
            			ip = null;
	            	}else if((m = titlePattern.matcher(line)).matches()){
	            		titleOut = new Text();
	            		title = m.group(1);
	            		titleOut.set(title);
	            		System.out.println("\tTITLE " + title);
	            	}else if((m = userNamePattern.matcher(line)).matches()){
	            		user = m.group(1);
	            		System.out.println("\tUSER " + user);
	            	}else if((m = ipPattern.matcher(line)).matches()){
	            		ip = m.group(1);
	            		System.out.println("\tIP USER " + ip);
	            	}
	            }
	        }
	        
	        public void configure(JobConf job) {

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

	        JobConf conf = new JobConf(getConf(), BuildUserEditGraph.class);
	        conf.setJobName(String.format("PreprocessWikiInput[%s: %s, %s: %s]", INPUT, inputPath, OUTPUT, outputPath));
	               

	        conf.setNumMapTasks(4);
	        conf.setNumReduceTasks(0);

	        conf.setMapperClass(LanguageMapper.class);
	        
	        //conf.setInputFormat(WikipediaPageInputFormat.class);
	        conf.setInputFormat(XMLInputFormatOld.class);
	        conf.setOutputFormat(TextOutputFormat.class);
	        //conf.setOutputFormat(TextOutputFormat.class);
	        
	        // Set heap space - using old API
	        conf.set("mapred.job.map.memory.mb", "2048");
	        conf.set("mapred.map.child.java.opts", "-Xmx2048m");
	        conf.set("mapred.job.reduce.memory.mb", "6144");
	        conf.set("mapred.reduce.child.java.opts", "-Xmx6144m");
	        conf.set("xmlinput.start","page");
	        conf.set("xmlinput.end","page");
	        //conf.set("mapred.child.java.opts", "-Xmx2048m");
	        
	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(PairOfStrings.class);
	        
	        FileSystem fs = FileSystem.get(conf);        
	        Path outPath = new Path(outputPath);
	        
	        // Job 1
	        FileInputFormat.setInputPaths(conf, new Path(inputPath));
	        FileOutputFormat.setOutputPath(conf, outPath);
	        
	        // Delete the output directory if it exists already.
	        fs.delete(outPath, true);

	        JobClient.runJob(conf);
	        
	        return 0;
	    }

	    public BuildUserEditGraph() {}

	    public static void main(String[] args) throws Exception {
	        ToolRunner.run(new BuildUserEditGraph(), args);
	    }
	}


