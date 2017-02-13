import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PDLocationMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String line = value.toString();
	    if(!line.startsWith("start")) {
		    String[] lineArray = line.split("\t");
		    String pickupPart = lineArray[0];
		    String dropoffPart = lineArray[1];
		    String[] pickupArray = pickupPart.split(",");
		    String[] dropoffArray = dropoffPart.split(",");
		    String hour = pickupArray[0].split(" ")[1].substring(0,2);
		    String pickupCensusCode = pickupArray[10];
		    String pickupNtaName = pickupArray[13];
		    String dropoffCensusCode = dropoffArray[10];
		    String dropoffNtaName = dropoffArray[13];
		    String censusKey = hour + "\t" + pickupCensusCode + "\t" + dropoffCensusCode;
		    String ntaNameKey = hour + "\t" + pickupNtaName + "\t" + dropoffNtaName;
		    context.write(new Text(censusKey), new Text("1"));
			context.write(new Text(ntaNameKey), new Text("1"));
		}
	}
}