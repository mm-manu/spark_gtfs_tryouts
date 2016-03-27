# This python script uses Apache Spark to calculate the number of stops and the distinct routes for each Bus/U-S-Bahn stop and prints out the sorted result.

from pyspark import SparkConf, SparkContext
import csv
import sys

# this function loads the stop.txt file and generate a dictionary to translate stop_id to stop_name
def loadStopNames():
    stopNames = {}
    try:
        with open("vbb_gtfs_data/stops.txt") as s:
            next(s)
            reader = csv.reader(s, delimiter=',')
            for row in reader:
                stopNames[int(row[0])]=row[2]
    except EnvironmentError:
        sys.exit("Please make sure that the GTFS files are in the data directory!")
    return stopNames
    
# this function loads the trips.txt file and generates a dictionary to translate trip_id to route_id
def loadTrip2route():
    trip2route = {}
    try:
        with open("vbb_gtfs_data/trips.txt") as t:
            next(t)
            reader = csv.reader(t, delimiter=',')
            for row in reader:
                trip2route[int(row[2])]=int(row[0])
    except EnvironmentError:
        sys.exit("Please make sure that the GTFS files are in the data directory!")
    return trip2route
    
# this function loads the routes.txt file and generates a dictionary to translate route_id to route_short_name
def loadRouteNames():
    routeInfos = {}
    try:
        with open("vbb_gtfs_data/routes.txt") as r:
            next(r)
            reader = csv.reader(r, delimiter=',')
            for row in reader:
                routeInfos[int(row[0])]=row[2]
    except EnvironmentError:
        sys.exit("Please make sure that the GTFS files are in the data directory!")
    return routeInfos
    

# setup spark config
conf = SparkConf().setMaster("local[*]").setAppName("TopJunctionsBerlin")
sc = SparkContext(conf = conf)

# make the dictionaries available to all cluster nodes
stopNamesDict = sc.broadcast(loadStopNames())
trip2route = sc.broadcast(loadTrip2route())
routeNames = sc.broadcast(loadRouteNames())

# load stop_times.txt, filter header, map each stop_id to (1,trip_id) tuples, translate trip_id to route_id and store it as a single-valued set
stops = sc.textFile("vbb_gtfs_data/stop_times.txt").filter(lambda x: 'stop_id' not in x).map(lambda x: (int(x.split(',')[3]),(1,int(x.split(',')[0])))).map(lambda (x,(y,z)): (x,(y,set([trip2route.value[z]]))))

# reduce by key: sum up 1's and union the route_id sets
stopCounts = stops.reduceByKey(lambda (x,y),(p,q): (x+p,y|q))

# sort by the number of stops: flip keys and values to apply sortByKey(), flip back
sortedByCount = stopCounts.map(lambda (x,(y,z)): (y,(x,z))).sortByKey().map(lambda (x,(y,z)): (y,(x,z)))

# use dictionary to translate stop_id into stop_name 
sortedByCountWithNames = sortedByCount.map(lambda (x,y): (stopNamesDict.value[x],y))

# collect the results
results = sortedByCountWithNames.collect()

# print the results
for result in results:
    print result[0]+" has "+str(result[1][0])+" stops from "+str(len(result[1][1]))+" distinct routes:"  
    for route_id in result[1][1]:
        name = routeNames.value[route_id]
        if len(name)==0:
            print "?",
        else:
            print name,
    print "\n"

# Sample output (? indicates unknown route_short_name, duplicates occur because multiple route_ids have the same route_short_name)
# ...
#S+U Pankow (Berlin) has 10040 stops from 18 distinct routes:
#N50 250 255 N2 50 S8 S8 S2 M1 S2 S9 M27 U2 155 S2 X54 107 S9
#
#S+U Hermannstr. (Berlin) has 10219 stops from 30 distinct routes:
#N8 U8 S42 S41 N79 U8 S42 S41 246 277 S41 S41 S41 S42 S42 S45 S45 S45 S45 S46 S46 S46 S46 S47 S47 S47 S47 370 377 M44
#
#Hertzallee (Berlin) has 10820 stops from 18 distinct routes:
#249 N1 N2 N9 N10 200 204 N26 X9 X10 X34 100 M45 M46 245 M49 109 110
#
#S+U Berlin Hauptbahnhof has 12829 stops from 40 distinct routes:
#M85 120 123 RB21 RB22 RB23 RB24 M10 M5 142 147 N40 ? U55 ? ? RE1 ? RB14 RE1 TXL RE7 245 RB24 RE1 N20 RE7 ? EV RE2 ? S5 S7 S75 S75 M8 M41 IRE IRE ? ?
#
#S+U Rathaus Spandau (Berlin) has 13204 stops from 22 distinct routes:
#N7 130 134 135 136 137 N30 N34 U7 638 638 X33 236 237 667 667 671 671 337 M32 M37 M45
#
#S+U Zoologischer Garten Bhf (Berlin) has 20012 stops from 50 distinct routes:
#N1 N2 RB14 RB21 N9 N10 RB24 N26 U1 U2 ? ? RE7 ? ? U9 ? A05 ? 110 RE1 204 X9 X10 RB22 245 RB23 RE1 249 RE1 X34 RE7 EV RE2 S46 M45 S5 M46 S7 S75 S75 200 RB24 100 IRE IRE ? ? M49 109 ?

