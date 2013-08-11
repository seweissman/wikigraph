package wikigraph;


import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

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
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;
import edu.umd.cloud9.io.pair.PairOfStrings;

	
	public class GetWikiStats extends Configured implements Tool {
	    private static final Logger LOG = Logger.getLogger(GetWikiStats.class);

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

		/*
		 * 
		 * 
		 * pp      <namespace key="-2" case="first-letter">Media</namespace>
<namespace key="-1" case="first-letter">Special</namespace>
*  <namespace key="0" case="first-letter" />
*  <namespace key="1" case="first-letter">Talk</namespace>
*  <namespace key="2" case="first-letter">User</namespace>
*  <namespace key="3" case="first-letter">User talk</namespace>
<namespace key="4" case="first-letter">Wikipedia</namespace>
<namespace key="5" case="first-letter">Wikipedia talk</namespace>
<namespace key="6" case="first-letter">File</namespace>
<namespace key="7" case="first-letter">File talk</namespace>
<namespace key="8" case="first-letter">MediaWiki</namespace>
<namespace key="9" case="first-letter">MediaWiki talk</namespace>
<namespace key="10" case="first-letter">Template</namespace>
<namespace key="11" case="first-letter">Template talk</namespace>
<namespace key="12" case="first-letter">Help</namespace>
<namespace key="13" case="first-letter">Help talk</namespace>
<namespace key="14" case="first-letter">Category</namespace>
<namespace key="15" case="first-letter">Category talk</namespace>
<namespace key="100" case="first-letter">Portal</namespace>
<namespace key="101" case="first-letter">Portal talk</namespace>
<namespace key="108" case="first-letter">Book</namespace>
<namespace key="109" case="first-letter">Book talk</namespace>
<namespace key="446" case="first-letter">Education Program</namespace>
<namespace key="447" case="first-letter">Education Program talk</namespace>
<namespace key="710" case="first-letter">TimedText</namespace>
<namespace key="711" case="first-letter">TimedText talk</namespace>
<namespace key="828" case="first-letter">Module</namespace>

		 */
	    

	    private static class ProfileMapper extends Mapper<Text, UserProfile, IntWritable, ArrayListOfLongsWritable> {

			public static final IntWritable ONE = new IntWritable(1);
			@Override
			public void map(Text userkey, UserProfile profile, Context context)
			       throws IOException, InterruptedException {
				//System.out.println("key = " + key);

				TreeMap<Integer,Long> nsmap = profile.getNamespaceMap();
				//TreeMap<Long,Long> articlemap = profile.getArticleMap();
				TreeMap<Long,Long> editmap = profile.getEditMap();
				long addbytes = profile.getBytesAdded();
				long removebytes = profile.getBytesRemoved();
				long nadds = profile.getNAddEdits();
				long nremoves = profile.getNRemoveEdits();
				long nedits = profile.getNEdits();
				long narticles = profile.getNArticles();
				long timetonext = profile.getTimeToNextEdit();
				long span = editmap.lastKey() - editmap.firstKey();

				ArrayListOfLongsWritable statlist = new ArrayListOfLongsWritable();
				statlist.add(addbytes);
				statlist.add(removebytes);
				statlist.add(nadds);
				statlist.add(nedits);
				statlist.add(nremoves);
				statlist.add(narticles);
				statlist.add(timetonext);
				statlist.add(span);
				long nscount;
				for(int i=0;i<=15;i++){
					nscount = 0;
					if(nsmap.containsKey(i)){
						nscount += nsmap.get(i);
					}
					statlist.add(nscount);
				}
				
				context.write(ONE,statlist);
				
			}
			
	    }
	    
	    private static class ProfileReducer extends Reducer<IntWritable, ArrayListOfLongsWritable, IntWritable, ArrayListOfLongsWritable> {
	    	public static final int nFields = 24;
			@Override
			public void reduce(IntWritable key, Iterable<ArrayListOfLongsWritable> counts, Context context)
			       throws IOException, InterruptedException {
				//System.out.println("key = " + key);
				
				ArrayListOfLongsWritable countsOut = new ArrayListOfLongsWritable();
				for(int i=0;i<nFields;i++){
					countsOut.add(0);
				}
				
				Iterator<ArrayListOfLongsWritable> countsIt = counts.iterator();
				
				while(countsIt.hasNext()){
					ArrayListOfLongsWritable countArray = countsIt.next();
					for(int i=0;i<nFields;i++){
						countsOut.set(i, countsOut.get(i) + countArray.get(i));
					}
				}
				context.write(key, countsOut);

				
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
	        job.setJobName(String.format("GetWikiStats[%s: %s, %s: %s]", INPUT, inputPath, OUTPUT, outputPath));
	               

	        job.setNumReduceTasks(1);

	        job.setMapperClass(ProfileMapper.class);
	        job.setReducerClass(ProfileReducer.class);
	        job.setCombinerClass(ProfileReducer.class);
	        
	        job.setInputFormatClass(SequenceFileInputFormat.class);
	        job.setOutputFormatClass(SequenceFileOutputFormat.class);
	        //conf.setOutputFormat(TextOutputFormat.class);
	        
	        
	        job.setOutputKeyClass(IntWritable.class);
	        job.setOutputValueClass(ArrayListOfLongsWritable.class);
	        
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
	        
	        return 0;
	    }

	    public GetWikiStats() {}

	    public static void main(String[] args) throws Exception {
	        ToolRunner.run(new GetWikiStats(), args);
	    }
	}


