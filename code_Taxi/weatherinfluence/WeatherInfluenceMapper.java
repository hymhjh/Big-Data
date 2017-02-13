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

public class WeatherInfluenceMapper extends Mapper<LongWritable, Text, Text, Text>{
         
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
                String datetime = weatherDataArray[0] + weatherDataArray[1];
                weatherDataMap.put(datetime, weatherDataArray[2]);
            }
        } catch(IOException ex) {
            System.err.println("Exception while reading weather data file: " + ex.getMessage());
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] lineArray = line.split(",");
        String pickupDateTime = lineArray[0];
        String[] pickupDateTimeArray = pickupDateTime.split(" ");
        String pickupDate = pickupDateTimeArray[0];
        String pickupHour = pickupDateTimeArray[1].substring(0,2);
        String pickupDateHour = pickupDate + pickupHour;
        //weekdays or weekends
        int year = Integer.valueOf(pickupDate.substring(0, 4));
        int month = Integer.valueOf(pickupDate.substring(4, 6));
        int day = Integer.valueOf(pickupDate.substring(6, 8));
        if (month < 3) {
            month += 12;
            year--;
        }
        int century = year / 100;
        int yearInCentury = year % 100;
        int w = (day + yearInCentury + (yearInCentury / 4) + (century / 4) + (5 * century) + ((month + 1) * 26 / 10)) % 7;
        //w = 0,1 means Saturday or Sunday,
        String weekKey = new String();
        if(w <= 1) {
            weekKey = "Weekends";
        } else if(w >= 2 && w <=6) {
            weekKey = "Weekdays";
        }
        if(weatherDataMap.containsKey(pickupDateHour)) {
            String weatherCondition = weatherDataMap.get(pickupDateHour);
            String reduceKey = weekKey + " " + pickupHour +" " + weatherCondition;
            context.write(new Text(reduceKey), new Text("1"));
        }
    }
}