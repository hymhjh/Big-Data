These codes compute the total number of bike that leave from a certain neighborhood per hour from 8:00 - 12:00 and 15:00 - 19:00 on 05/02/2016. 

There are two types of neighborhoods we use: NTA and Borough census tract.

Due to the limitation of longitude/latitude to neighborhood conversion, we use the citibike data on 05/02/2016. And convert longitude/latitude to neighborhood using Arcgis. 

We have put the preprocessed data in dumbo: /scratch/gc1779/project/Intersect of 201605-citibike-tripdata_0502_start and NYC_Census_Tracts_2010_0.csv

Mapper: 
CountMapper.java

Reducer:
CountReducer.java

driver:
Counter.java