# Databricks notebook source
# Kirtan Pokiya

# COMMAND ----------

sc = spark.sparkContext

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/assignment_1"))

# COMMAND ----------

data = sc.wholeTextFiles("dbfs:/FileStore/tables/assignment_1")
data.collect()

# COMMAND ----------

rdd = data.flatMap(lambda x: x[1].strip().split("\n"))
rdd = rdd.map(lambda x: x.split(","))
rdd = rdd.map(lambda x: (x[0], float(x[1]), float(x[2])))
rdd.collect()

# COMMAND ----------

from math import floor, sqrt

threshold = .75
def form_grid(point):
    pt_id, x, y = point
    x_cell = floor(x/threshold)
    y_cell = floor(y/threshold)
    neighbor_cell = [(x_cell + i, y_cell + j) for i in range(2) for j in range(2)]
    grid = (((cx, cy), point) for cx, cy in neighbor_cell)
    return grid

grid = rdd.flatMap(form_grid)
grid = grid.groupByKey()

grid.persist()

# COMMAND ----------

import itertools

def distance(point1, point2):
    _, x1, y1 = point1
    _, x2, y2 = point2
    return sqrt((x2-x1)**2+(y2-y1)**2)

processed_pairs = {}
def closePairs(points):
    pairs = []
    for point1, point2 in itertools.combinations(points, 2):
        if point1!=point2:
            sorted_pair = tuple(sorted([point1[0], point2[0]]))
            dist = distance(point1, point2)
            if dist <= threshold and sorted_pair not in processed_pairs:
                pairs.append((sorted_pair, dist))
                processed_pairs[sorted_pair] = True
    return pairs
ans = grid.flatMap(lambda x: closePairs(x[1]))

# COMMAND ----------

ans = grid.flatMap(lambda x: closePairs(x[1]))

grid.unpersist()

ans.collect()
ans.take(8)

# COMMAND ----------

for (point1, point2), dist in ans.take(8):
    print("Dist: ", dist)
    print("2 ",point1,",", point2)

# COMMAND ----------

pair_with_min_distance = ans.reduce(lambda pair1, pair2: pair1 if pair1[1] < pair2[1] else pair2)
pair_with_min_distance
