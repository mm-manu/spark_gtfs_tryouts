###Description
This repository contains some Python scripts I wrote to become familiar with Apache Spark and GTFS.
* ```trips_per_route.py```: <br />
calculates the number of trips for each route and returns the result ordered by number of trips.  
Output format:  ```Route_ID <route_id> (<route_short_name>) operated by <agency_id> contains <#trips> ```
* ```stops_and_routes_per_stop.py```: <br />
calculates the number of stop and the number of distinct routes for each stop location and returns the result ordered by number of stops.<br />
  Output format:  ```<stop_name> has <#stops> from <#distinct_routes> routes: <route_short_names> ```

###Requirements
* JDK
* Spark

###Usage
* Unzip the data file
* ```spark-submit <name_of_script>.py```
