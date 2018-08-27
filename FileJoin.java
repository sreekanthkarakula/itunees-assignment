import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

public class FileJoin {
	
	/**
	 * below is the implementaion of left join with 2 files as input to the program 
	 * select id, first_name, last_name, salary, bonus from Employee_name LEFT JOIN Employee_pay ON Employee_name.id = Employee_pay.id
	 * @param args, file1 and file2 
	 * @throws IOException
	 */
	
	public static void main(String args[]) throws IOException {
		
		String file1= args[0];
		
		String file2 = args[1];
		
		File outputFile = new File(args[2]);
		
		boolean header = true;
		
		FileOutputStream writer = new FileOutputStream(outputFile);
		
		try (BufferedReader filereader = new BufferedReader(new FileReader(file1))) {
			
		    String line;
		    while ((line = filereader.readLine()) != null) {
		    	
		    	if(header) {
		    		header = false;
		    		continue;
		    	}
		    	
		    	 String[] ids = line.split(",");
		    	 String id1 = ids[0];
		    	 
		 		boolean headerinfile2 = true;
		    	 
		    	 try (BufferedReader filereaderforFile2 = new BufferedReader(new FileReader(file2))) {
		    		 String line2;
		    		 while ((line2 = filereaderforFile2.readLine()) != null) {
		    			 
		    			 if(headerinfile2) {
		    				 headerinfile2 = false;
		 		    		 continue;
		 		    	}
		    			 String[] file2ids = line2.split(",");
		    			 System.out.println("comparing the values " + ids[0] + " and " +  file2ids[0]);
		    			 
		    			 if(id1.equals( file2ids[0])) {
		    				 //
		    				 StringBuilder builder = new StringBuilder();
		    				 for(int i=0; i<ids.length; i++ ) {
		    					 builder.append(ids[i]);
			    				 builder.append(',');
		    				 }
		    				 builder.append(file2ids[1]);
		    				 builder.append(',');
		    				 System.out.println("the length of arr is " + file2ids.length);
		    				 if(file2ids.length > 2 ) {
		    					 builder.append(file2ids[2]);
		    				 }
		    				 else  builder.append("null");
		    				 builder.append("\n");

		    					 System.out.println("writing to file ");
		    					 writer.write(builder.toString().getBytes());
		    			 }
		    			 else {
		    				 StringBuilder builder = new StringBuilder();
		    				 for(int i=0; i<ids.length; i++ ) {
		    					 builder.append(ids[i]);
			    				 builder.append(',');
		    				 }
		    				 builder.append("null");
		    				 builder.append(',');
		    				 builder.append("null");
		    				 builder.append(',');
		    				 builder.append("null");
		    				 builder.append("\n");
		    			 }
		    		 }
		    		 filereaderforFile2.close();
			    	 }
		    }
		    filereader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			writer.close();
			
		}
	}

}
