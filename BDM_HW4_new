# initialize spark context
from pyspark import SparkContext
sc = SparkContext()

import fiona
import fiona.crs
import shapely
import rtree
import pandas as pd
import geopandas as gpd
import sys


def parse_bdm_file(input_file,output_path):
	# Construct an R-Tree as demonstrated in Lab 7, 
	# with the key for each bound being the index into the neighborhood name
	# neighborhoods = gpd.read_file('neighborhoods.geojson').to_crs(fiona.crs.from_epsg(2263))
	# index = rtree.Rtree()
	# for idx,geometry in enumerate(neighborhoods.geometry):
	# 	index.insert(idx, geometry.bounds)

 #    # This is the bounding box of all neighborhoods (in NAD 83 projection)
	# index.bounds #bounding area of all bounds

	def createIndex(shapefile):
		import rtree
		import fiona.crs
		import geopandas as gpd
		zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
		index = rtree.Rtree()
		for idx,geometry in enumerate(zones.geometry):
			index.insert(idx, geometry.bounds)
		return (index, zones)

	def findZone(p, index, zones):
		match = index.intersection((p.x, p.y, p.x, p.y))
		for idx in match:
			if zones.geometry[idx].contains(p):
				return idx
		return None

	def processTrips(pid, records):
		import csv
		import pyproj
		import shapely.geometry as geom
		# Create an R-tree index
		proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
		index, zones = createIndex('neighborhoods.geojson')

		# Skip the header
		if pid==0:
			next(records)
		reader = csv.reader(records)
		counts = {}

		for row in reader:
			try: 
				# obtain pickup and dropoff coordinates
				pickup_coords = geom.Point(proj(float(row[5]), float(row[6]))) # long, lat
				dropoff_coords = geom.Point(proj(float(row[9]), float(row[10]))) # long, lat

				# some rows have pickups or dropoffs that are not in NYC
				try:
					pickup_idx = findZone(pickup_coords, index, zones)
					dropoff_idx = findZone(dropoff_coords, index, zones)
					if (pickup_idx != None) & (dropoff_idx != None): # only update counts with trips that start and end in NYC
						pickup_borough = zones['borough'][pickup_idx]
						dropoff_neighborhood = zones['neighborhood'][dropoff_idx]
						# update counts dictionary
						if (pickup_borough, dropoff_neighborhood):
							counts[(pickup_borough, dropoff_neighborhood)] = counts.get((pickup_borough, dropoff_neighborhood), 0) + 1
				except:
					pass
			except:
				pass
		return counts.items()
	rdd = sc.textFile(input_file)
	counts = rdd.mapPartitionsWithIndex(processTrips)\
				.reduceByKey(lambda x, y: x+y)\
				.map(lambda x: (x[0][0], x[0][1], x[1]))\
				.sortBy(lambda x: x[2], False)\
				.groupBy(lambda x: x[0]).mapValues(list)\
				.sortBy(lambda x: x[0], True)\
				.map(lambda x: x[1][:3])\
				.map(lambda x: (x[0][0], x[0][1], x[0][2], x[1][1], x[1][2], x[2][1], x[2][2]))\
				.saveAsTextFile(output_path)
if __name__ == '__main__':
	input_file = sys.argv[1]
	output_path = sys.argv[2]
	parse_bdm_file(input_file,output_path)
