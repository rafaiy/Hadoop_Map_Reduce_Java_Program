package accidentanalysis;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;


public class analysis {
	
	public static class maxaccidentmap extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable arg0, Text value, Context context) throws IOException, InterruptedException{
			
			
			String line = value.toString();
			String Columns="States/Uts,Total No. of Road Accidents 2006,Total No. of Road Accidents 2007,Total No. of Road Accidents 2008,Total No. of Road Accidents 2009,Total No. of Road Accidents 2010,Total No. of Road Accidents 2011,Total No. of Road Accidents 2012,Total No. of Road Accidents 2013,Total No. of Road Accidents 2014,Total No. of Road Accidents 2015,Number of Persons Killed 2006,Number of Persons Killed 2007,Number of Persons Killed 2008,Number of Persons Killed 2009,Number of Persons Killed 2010,Number of Persons Killed 2011,Number of Persons Killed 2012,Number of Persons Killed 2013,Number of Persons Killed 2014,Number of Persons Killed 2015,Number of Persons Injured 2006,Number of Persons Injured 2007,Number of Persons Injured 2008,Number of Persons Injured 2009,Number of Persons Injured 2010,Number of Persons Injured 2011,Number of Persons Injured 2012,Number of Persons Injured 2013,Number of Persons Injured 2014,Number of Persons Injured 2015";
			String[] Column= Columns.split(",");
			int min=0,index=0;
			if (!(line.length() == 0)) {
				
				String[] val= line.split(",");
				
				for(int i=1;i<val.length;i++){
					int columnval = Integer.parseInt(val[i]);
					if(min < columnval){
						min=columnval;	
						index=i;			
						
					                   }
                                               }
				
	context.write(new Text("state" + String.valueOf(val[0])+ "min" + String.valueOf(Column[index])), new Text(String.valueOf(min)));
				}}}
		
		
		
		public static class maxaccidentred extends Reducer<Text, Text, Text, Text> {

	/**
	* @method reduce
	* This method takes the input as key and list of values pair from mapper, it does aggregation
	* based on keys and produces the final context.
	*/
	
	public void reduce(Text Key, Iterator<Text> Values, Context context)
			throws IOException, InterruptedException {

		
		//putting all the values in temperature variable of type String
		
		String got = Values.next().toString();
		context.write(Key, new Text(got));
	}

}
	




	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		
        Job job = new Job(conf, "Accident");
		
		//Assigning the driver class name
		job.setJarByClass(analysis.class);

		//Key type coming out of mapper
		job.setMapOutputKeyClass(Text.class);
		
		//value type coming out of mapper
		job.setMapOutputValueClass(Text.class);

		//Defining the mapper class name
		job.setMapperClass(maxaccidentmap.class);
		
		//Defining the reducer class name
		job.setReducerClass(maxaccidentred.class);

		//Defining input Format class which is responsible to parse the dataset into a key value pair
		job.setInputFormatClass(TextInputFormat.class);
		
		//Defining output Format class which is responsible to parse the dataset into a key value pair
		job.setOutputFormatClass(TextOutputFormat.class);

		//setting the second argument as a path in a path variable
		Path OutputPath = new Path(args[1]);

		//Configuring the input path from the filesystem into the job
		FileInputFormat.addInputPath(job, new Path(args[0]));

		//Configuring the output path from the filesystem into the job
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//deleting the context path automatically from hdfs so that we don't have delete it explicitly
		OutputPath.getFileSystem(conf).delete(OutputPath);

		//exiting the job only if the flag value becomes false
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
