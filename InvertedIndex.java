package mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

	public static class InvertedIndexMapper
    extends Mapper<Object, Text, Text, Text>{

 public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
	 String document=value.toString();
	 String[] arr = document.split("\\t");
	 String docId = arr[0];
	 StringTokenizer st = new StringTokenizer(arr[1]);  
     while (st.hasMoreTokens()) {   
    	 context.write(new Text(st.nextToken()), new Text(docId));
     }  
 }
}

public static class InvertedIndexReducer
    extends Reducer<Text,Text,Text,Text> {
 public void reduce(Text key, Iterable<Text> values,
                    Context context
                    ) throws IOException, InterruptedException {
	 HashMap<String, Integer> map = new HashMap<String, Integer>();
	 int count=0;
	 for(Text t:values){
		 String str=t.toString();
		 if( map != null && map.get(str) != null){
			 count = map.get(str);
			 count++;
			 map.put(str, count);
		 } else {
			 map.put(str, 1);
		 }
		 
	 }
	 StringBuilder stringBuilder = new StringBuilder();
	 for(HashMap.Entry m: map.entrySet()){
		 stringBuilder.append(m.getKey()+":"+m.getValue()+"\t");
	 }
	 context.write(key, new Text(stringBuilder.toString()));
	 
 }
}

	 public static void main(String[] args) throws Exception {
		 if(args.length != 2){
			 System.err.println("Usage: InvertedIndex <Input path> <Output path>");
			 System.exit(-1);
		 }
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "InvertedIndex");
		    job.setJarByClass(InvertedIndex.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setMapperClass(InvertedIndexMapper.class);
		    job.setReducerClass(InvertedIndexReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    Path outputPath = new Path(args[1]);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, outputPath);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
