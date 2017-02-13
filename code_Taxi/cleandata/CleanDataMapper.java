import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanDataMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String line = value.toString();
	    String[] lineArray = line.split(",");
        String pickupDatetime = lineArray[1];
	    String[] pickupDatetimeArray = pickupDatetime.split(" ");
	    if(pickupDatetimeArray.length == 2) {
	    	String pickupDate = pickupDatetimeArray[0];
	    	String pickupTime = pickupDatetimeArray[1];
	    	String[] pickupDateArray = pickupDate.split("-");
	    	if(pickupDateArray.length == 3) {
	            if(pickupDateArray[1].length() == 1) {
	    	    	pickupDate = pickupDateArray[0] + "0" + pickupDateArray[1];
	            } else {
	            	pickupDate = pickupDateArray[0] + pickupDateArray[1];
	            }
	            if(pickupDateArray[2].length() == 1) {
	                pickupDate += "0" + pickupDateArray[2];
	            } else {
	                pickupDate += pickupDateArray[2];
	            }
			}
	        String[] pickupTimeArray = pickupTime.split(":");
	        if(pickupTimeArray[0].length() == 1) {
	            pickupTime = "0" + pickupTimeArray[0];
	        } else {
	            pickupTime = pickupTimeArray[0];
	        }
			pickupTime = pickupTime + pickupTimeArray[1] + pickupTimeArray[2];
	        pickupDatetime = pickupDate + " " + pickupTime;

	        String dropoffDatetime = lineArray[2];
	        String[] dropoffDatetimeArray = dropoffDatetime.split(" ");
	        if(dropoffDatetimeArray.length == 2) {
	            String dropoffDate = dropoffDatetimeArray[0];
	            String dropoffTime = dropoffDatetimeArray[1];
	            String[] dropoffDateArray = dropoffDate.split("-");
	 	    	if(dropoffDateArray.length == 3) {
	    	    	if(dropoffDateArray[1].length() == 1) {
	    			dropoffDate = dropoffDateArray[0] + "0" + dropoffDateArray[1];
	    	    	} else {
	        		dropoffDate = dropoffDateArray[0] + dropoffDateArray[1];
	    	   		}
	    	    	if(dropoffDateArray[2].length() == 1) {
	               		dropoffDate += "0" + dropoffDateArray[2];
	    	    	} else {
	        		dropoffDate += dropoffDateArray[2];
	            	}
		    	}
	            String[] dropoffTimeArray = dropoffTime.split(":");
	            if(dropoffTimeArray[0].length() == 1) {
	                dropoffTime = "0" + dropoffTimeArray[0];
	            } else {
	                dropoffTime = dropoffTimeArray[0];
	            }
		    	dropoffTime = dropoffTime + dropoffTimeArray[1] + dropoffTimeArray[2];
		    	dropoffDatetime = dropoffDate + " " + dropoffTime;

	            String passengerCount = lineArray[3];
	    	    String tripDistance = lineArray[4];
		    	String pickupLongitude = lineArray[5];
		    	String pickupLatitude = lineArray[6];
		    	String dropoffLongitude = lineArray[9];
		    	String dropoffLatitude = lineArray[10];
		    	String fareAmount = lineArray[12];

		    	if(pickupLongitude.length() != 1 && pickupLatitude.length() != 1 && dropoffLongitude.length() != 1 && dropoffLatitude.length() != 1 && Integer.valueOf(passengerCount) != 0 && Double.valueOf(tripDistance) != 0 && Double.valueOf(fareAmount) != 0 && pickupDateArray.length == 3 && dropoffDateArray.length == 3 && !pickupDatetime.equals(dropoffDatetime)) {
					String result = pickupDatetime + "," + dropoffDatetime + "," + passengerCount + "," + tripDistance + "," + pickupLongitude + "," + pickupLatitude + "," + dropoffLongitude + "," + dropoffLatitude;
					context.write(new Text(), new Text(result));
		    	}
			}
	    }
	}
}
