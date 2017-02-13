import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PerHourMapper extends Mapper<LongWritable, Text, Text, Text>{
         
    private Map<String, String> weatherDataMap = new HashMap<>();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try{
            Path[] weatherDataFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if(weatherDataFiles != null && weatherDataFiles.length > 0) {
                for(Path weatherDataFile : weatherDataFiles) {
                    readFile(weatherDataFile);
                }
            }
        } catch(IOException ex) {
                System.err.println("Exception in mapper setup: " + ex.getMessage());
        }
    }

    private void readFile(Path filePath) {
        try{
            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
            String weatherData = null;
            while((weatherData = bufferedReader.readLine()) != null) {
                String[] weatherDataArray = weatherData.split("\\s+");
                String mapKey = weatherDataArray[0] + " " + weatherDataArray[1] + " " + weatherDataArray[2];
                weatherDataMap.put(mapKey, weatherDataArray[3]);
            }
        } catch(IOException ex) {
            System.err.println("Exception while reading weather_24 data file: " + ex.getMessage());
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] lineArray = line.split(" ");
        String timeWeather = lineArray[0] + " " + lineArray[1] + " " + lineArray[2];
        if(weatherDataMap.containsKey(timeWeather)) {
            int hour = Integer.valueOf(weatherDataMap.get(timeWeather));
            int totalNumber = Integer.valueOf(lineArray[3]);
            int perHour = totalNumber / hour;
            String reduceKey = timeWeather + ": " + String.valueOf(perHour);
            context.write(new Text(reduceKey), new Text("1"));
        }
    }
}