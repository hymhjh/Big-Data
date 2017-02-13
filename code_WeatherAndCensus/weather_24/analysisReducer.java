import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class analysisReducer extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
		String[] weather = new String[24];
		
		for(Text value : values){
			String line = value.toString();
			String[] sp = line.split(",");
			int hour = Integer.parseInt(sp[0].substring(0, 2));
			if(sp[2].contains("RA")) {
				if(sp[3].equals(" ") || sp[3].contains("T") || sp[3].contains("M")){
					weather[hour] = "Light_Rain";
				}else{
					if(Double.parseDouble(sp[3]) < 0.098) weather[hour] = "Light_Rain";
					else if	(Double.parseDouble(sp[3]) < 0.3) weather[hour] = "Moderate_Rain";
					else weather[hour] = "Heavy_Rain";
				}
			}
			else{
				if(sp[2].contains("SN")) weather[hour] = "Snow";
				else if(sp[2].contains("FG") || sp[2].contains("BR") || sp[1].contains("OVC")) weather[hour] = "Cloudy";
				else if(sp[1].contains("CLR") || sp[1].contains("VV") || sp[1].contains("SCT") || 
						sp[1].contains("BKN") || sp[1].contains("FEW")) weather[hour] = "Clear";
				else {
					weather[hour] = "NA";
				}
			}
		}
		for(int i=0; i<24; i++){
			String start = "";
			if(i < 10) start = "0" + String.valueOf(i);
			else start = String.valueOf(i);
			if(weather[i] != null) context.write(new Text(key+" "+start+" "+weather[i]), new Text());
			else context.write(new Text(key+" "+start+" NA"), new Text());
		}
	}
}

