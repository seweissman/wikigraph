package wikigraph;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.XMLInputFormatOld;
	
	public class BuildUserProfile extends Configured implements Tool {
	    private static final Logger LOG = Logger.getLogger(BuildUserProfile.class);

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
	    
	    
	    private static class RevisionMapper extends MapReduceBase implements
	    Mapper<LongWritable, Text, Text, RevisionRecord> {
	    	 /*
	        <revision>
	        <id>4407235</id>
	        <parentid>2527990</parentid>
	        <timestamp>2004-02-25T18:55:21Z</timestamp>
	        <contributor>
	          <username>Dori</username>
	          <id>6878</id>
	        </contributor>
	        <minor/>
	        <comment>restoring blanked content, no reason given</comment>
	        <text id="4407235" bytes="538" />
	        <sha1>cgc468uc4empeg5jggze1lsb87vquuc</sha1>
	        <model>wikitext</model>
	        <format>text/x-wiki</format>
	      </revision>
	    */
	    	static final Pattern titlePattern = Pattern.compile(".*<title>(.*)<\\/title>.*");
	    	static final Pattern beginRevisionPattern = Pattern.compile(".*<revision>.*");
	    	static final Pattern endRevisionPattern = Pattern.compile(".*<\\/revision>.*");
	    	static final Pattern userNamePattern = Pattern.compile(".*<username>(.*)<\\/username>.*");
	    	static final Pattern ipPattern = Pattern.compile(".*<ip>(.*)<\\/ip>.*");
	    	static final Pattern nsPattern = Pattern.compile(".*<ns>(.*)<\\/ns>.*");
	    	static final Pattern timestampPattern = Pattern.compile(".*<timestamp>(.*)<\\/timestamp>.*");
	    	static final Pattern bytesPattern = Pattern.compile(".*<text id=\"(.*)\" bytes=\"(.*)\" \\/>.*");
	           public void map(LongWritable key, Text p, OutputCollector<Text, RevisionRecord> output,
	                    Reporter reporter) throws IOException {
	               
	               // users & articles with weights?
	        	   // users to users consecutive edits with weights
	        	   // some sort of user editing diagram
	        	   // time to next edit
	        	   // highly protected/controversial articles
	            String lines[] = p.toString().split("\n");
	            //System.out.println("key = " + key);
	            Matcher m;
	            String title = null;
	            String user = null;
	            String ip = null;
	            String ns = null;
	            String timestamp = null;
	            String bytes = null;
	            Text userOut = null;
	            for(String line: lines){
					//System.out.println("LINE " + line);
	            	m = titlePattern.matcher(line);
	            	if((m = endRevisionPattern.matcher(line)).matches()){
						//System.out.println("\tend revision\n");
						//System.out.println("user = " + ((user != null)?user:"NULL"));
						//System.out.println("ip = " + ((ip != null)?ip:"NULL"));
						//<timestamp>2001-01-21T02:12:21Z</timestamp>
						//<ns>0</ns>

	            		if(title != null && (user != null || ip != null)
	            				&& ns != null && timestamp != null && bytes != null){
		            		Text name = new Text();
	            			if(user == null){
	            				name.set(ip);
	            			}else{
	            				name.set(user);
	            			}
	            			userOut = new Text();
	            			userOut.set(name);
	            			RevisionRecord r = new RevisionRecord(Integer.parseInt(ns),parseTime(timestamp),title,Integer.parseInt(bytes));
	            			output.collect(userOut, r);
						}
	            		bytes = null;
	            		user = null;
            			ip = null;
            			timestamp = null;
	            	}else if((m = titlePattern.matcher(line)).matches()){
	            		title = m.group(1);
	            		//System.out.println("\tTITLE " + title);
	            	}else if((m = userNamePattern.matcher(line)).matches()){
	            		user = m.group(1);
	            		//System.out.println("\tUSER " + user);
	            	}else if((m = ipPattern.matcher(line)).matches()){
	            		ip = m.group(1);
	            		//System.out.println("\tIP USER " + ip);
	            	}else if((m = nsPattern.matcher(line)).matches()){
	            		ns = m.group(1);
	            		//System.out.println("\tNS " + ns);
	            	}else if((m = timestampPattern.matcher(line)).matches()){
	            		timestamp = m.group(1);
	            		//System.out.println("\tTIMESTAMP " + timestamp);
	            	}else if((m = bytesPattern.matcher(line)).matches()){
	            		bytes = m.group(2);
	            		//System.out.println("\tBYTES " + bytes);
	            	}
	            }
	        }
	        
	        public void configure(JobConf job) {

	        }
	    }

	    
	    private static class RevisionReducer extends MapReduceBase implements
	    Reducer<Text, RevisionRecord, Text, UserProfile> {

			@Override
			public void reduce(Text key, Iterator<RevisionRecord> records,	OutputCollector<Text, UserProfile> profiles, Reporter arg3)
					throws IOException {

				
			}
	    	
	    }
	    
	    //<timestamp>2004-02-25T18:55:21Z</timestamp>
	    //yyyy-MM-dd'T'HH:mm:ssz
       public static long parseTime(String timestr) {
          
          Calendar calendar = javax.xml.bind.DatatypeConverter.parseDateTime(timestr);
		  return calendar.getTimeInMillis();
          /*
		  Date t; 
          try {
			t = ft.parse(timestr);
			return t.getTime();
		  } catch (java.text.ParseException e) {
				// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
		  }	
		  */ 
	       
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

	        JobConf conf = new JobConf(getConf(), BuildUserProfile.class);
	        conf.setJobName(String.format("PreprocessWikiInput[%s: %s, %s: %s]", INPUT, inputPath, OUTPUT, outputPath));
	               

	        conf.setNumMapTasks(4);
	        conf.setNumReduceTasks(20);

	        conf.setMapperClass(RevisionMapper.class);
	        
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
	        conf.setOutputValueClass(RevisionRecord.class);
	        
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

	    public BuildUserProfile() {}

	    public static void main(String[] args) throws Exception {
	        ToolRunner.run(new BuildUserProfile(), args);
	    }
	}


