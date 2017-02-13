Taxi part code explanation:

All the input data you can find in Dumbo cd /scratch/ys2312

1.CleanData: this program is used to clean and file the data, filter the data we don't want. Make the pick up date and time and drop off date and time to be the format "19990909 123210". The "1999" is the year, "09" is the month and the next "09" is the day. The "12" is the hour, "32" is the minute and "10" is the second.
If the pick up longitude and latitude is not "0", the drop off longitude and latitude is not "0", the passengerCount is not "0" tripdistance is not "0", fareAmount is not "0", the pick up date is not "0", the drop off date is not "0" and the pick up date is not equal to drop off date, this record would be stored. Otherwise it would be discarded.

2.TotalDayAndAvgSpeed: this program is used to count the number of taxi trips each day through Monday to Sunday, count the number of taxi trips each hour from 0 o'clock to 24 o'clock on weekdays and on weekends, and calculate the speed of every hour from 0 o'clock to 24 o'clock on weekdays and on weekends.

3.PDLocation: this program is to count the number of the pick up and drop off pairs of particular hour and particular CensusCode and count the number of pick up and drop off pairs of particular hour and particular Nta Name.

4.FindLocation: this program is to count the number of taxi trips of different census track areas and different hours and count the number of taxi trips of different nta areas and different hours.

5.Block: this program is to find the all the taxi records of particular day and particular hour.

6.WeatherInfluence: this program is to count the total number of taxi trips of every weather condition for each hour from 0 o'clock to 24 o'clock on weekdays and on weekends. This program uses the output of cleandata and the weather information of the every hour in 2015 as input.

7.PerHour: this program is to count the average number of taxi trips of every weather condition for each hour from 0 o'clock to 24 o'clock on weekdays and on weekends. This program uses the output of WeatherInfluence and the weather infomation that contains the total hour of particular weather condition in 2015 on a particular hour of the day on weekends and on weekdays.