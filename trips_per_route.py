# This python script counts the number of trips for each route and displays additional route information
import sys
from pyspark import SparkConf, SparkContext

def loadRouteInfos():
    routeInfos = {}
    try:
        with open("vbb_gtfs_data/routes.txt") as r:
            next(r)
            for line in r:
                fields = line.split(',')
                routeInfos[int(fields[0])] = (fields[1],fields[2])
    except EnvironmentError:
        sys.exit("Please make sure that the GTFS files are in the data directory!")
    return routeInfos


#setup spark config
conf = SparkConf().setMaster("local[*]").setAppName("Trips per route")
sc = SparkContext(conf = conf)
#make the dictionary available to all cluster nodes
routeInfos = sc.broadcast(loadRouteInfos())

# load trips.txt, filter header, extract (route_id, trip_id) tuples , transform values to single-elemented lists, reduce by key and join lists
tripsInRoutes = sc.textFile("vbb_gtfs_data/trips.txt").filter(lambda x: 'route_id' not in x).map(lambda x: (int(x.split(',')[0]),int(x.split(',')[2]))).map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y: x+y)
# count number of trips for each route
tripCounts = tripsInRoutes.map(lambda x: (x[0],len(x[1])))
# order by number of trips
orderedTripCounts = tripCounts.map(lambda (x,y): (y,x)).sortByKey().map(lambda (x,y): (y,x))
# include route info
orderedTripCountsWithInfo = orderedTripCounts.map(lambda (x,y): (x,y,routeInfos.value[x]))
# collect the results
results = orderedTripCountsWithInfo.collect()

# print results
for result in results:
    print "Route_ID "+str(result[0])+" ("+str(result[2][1])+") operated from "+str(result[2][0])+" contains "+str(result[1])+" trips."
    
# Sample output:
# ...
#Route_ID 538 (U6) operated from BVU--- contains 1727 trips.
#Route_ID 381 (M49) operated from BVB--- contains 1774 trips.
#Route_ID 539 (U7) operated from BVU--- contains 1882 trips.
#Route_ID 524 (M10) operated from BVT--- contains 1923 trips.
#Route_ID 376 (M41) operated from BVB--- contains 1966 trips.
#Route_ID 377 (M44) operated from BVB--- contains 2078 trips.
#Route_ID 378 (M45) operated from BVB--- contains 2171 trips.
#Route_ID 533 (U2) operated from BVU--- contains 2200 trips.
#Route_ID 297 (186) operated from BVB--- contains 2252 trips.
#Route_ID 528 (M4) operated from BVT--- contains 2280 trips.
#Route_ID 432 (U8) operated from BVB--- contains 2578 trips.
#Route_ID 540 (U8) operated from BVU--- contains 3424 trips.