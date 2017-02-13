import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class analysisReducer extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
		int[] weather = new int[7];
		String[] w = {"Clear","Cloudy","Light_Rain","Moderate_Rain","Heavy_Rain","Snow","NA"};
		for(Text value : values){
			if(value.toString().contains("Clear")) weather[0]++;
			else if(value.toString().contains("Cloudy")) weather[1]++;
			else if(value.toString().contains("Light Rain")) weather[2]++;
			else if(value.toString().contains("Moderate Rain")) weather[3]++;
			else if(value.toString().contains("Heavy Rain")) weather[4]++;
			else if(value.toString().contains("Snow")) weather[5]++;
			else weather[6]++;
		}
		for(int i=0; i<7; i++){
			context.write(key, new Text(w[i]+"\t"+weather[i]));
		}
	}
}
