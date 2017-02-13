These codes compute the average speed of bikes per hour in different weather on weekends and weekdays separately. There are two MapReduce jobs. The first MapReduce job takes the weather input and bike input, combine the weather data and bike data which are in the same hour, then send the output to second MapReduce job. 

We use the citibike data in 2015. You can find the data in https://s3.amazonaws.com/tripdata/index.html

First MapReduce job: 
BikeMapper.java
WeatherMapper.java
AnalyzerReducer.java

Second MapReduce job:
CountMapper.java
CountReducer.java

driver:
Analyzer.java