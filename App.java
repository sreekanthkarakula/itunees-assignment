package com.sree.hadoop.leftouterJoin;

/**
 * driver program for hadoop job
 *
 */
public class App extends Configured implements Tool{
	
    public static void main( String[] args )
    {
    	//define job
    	Configuration c=new Configuration();
		Path p1=new Path(args[0]);
		Path p2=new Path(args[1]);
		Path p3=new Path(args3[2]);

		Job job = new Job(c,"Files_Inner_Join ");
		job.setJarByClass(App.class);
		
		//set input and output formats
		
		MultipleInputs.addInputPath(job, p1, TextInputFormat.class, MapperForFile1.class);
		MultipleInputs.addInputPath(job,p2, TextInputFormat.class, MapperForFile2.class);
		job.setReducerClass(MultipleReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, p3);
		
		//job.run();
		job.waitForCompletion(true);
		
    }
    
    public static class MapperForFile1 extends Mapper
    {
     public void map(LongWritable key, Text value, Context context)
     throws IOException, InterruptedException
     {
    	  String[] line=value.toString().split(",");
          StringBuilder csv_forakey = new StringBuilder();
          for(i =1; i<line.length; i++) {
        	  csv_forakey.append(line[i]);
        	  csv_forakey.append(",");
          }
          csv_forakey.setLength(csv_forakey.length() - 1);
          context.write(new Text(line[0]), csv_forakey);
     }
    }


    public static class MapperForFile2 extends Mapper
    {
     public void map(LongWritable key, Text value, Context context)
     throws IOException, InterruptedException
     {
      String[] line=value.toString().split(",");
      StringBuilder csv_forakey = new StringBuilder();
      for(i =1; i<line.length; i++) {
    	  csv_forakey.append(line[i]);
    	  csv_forakey.append(",");
      }
      csv_forakey.setLength(csv_forakey.length() - 1);
      context.write(new Text(line[0]), csv_forakey);
     }
    }

    public static class CounterReducer extends Reducer {
    	
     StringBuilder line=null;

     public void reduce(Text key, Iterable values, Context context ) 
     throws IOException, InterruptedException
     {
      
      //for a given key if the length of string builder is greater than 1 then 
      
      int length =0;
      if (values instanceof Collection<?>) {
    	  length = ((Collection<?>)values).size();
      }
    
      if(length == 1) {
    	  //the record/id is only in left table not in right table; assuming the record/id will be in employee_names or all the id's in employee_pay is in employee_name
    	  for(Text value:values)
          {
           line.append(value.toString()) ;
          }
    	  line.append("null","null");
    	  
      }else {
    	  for(Text value:values)
          {
              line.append(value.toString()) ;
          }
      }
      context.write(key, new Text(line));
    }
   }
}
