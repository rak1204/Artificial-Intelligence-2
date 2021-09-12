import MapReduce
import sys

"""
JOIN in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

# Implement the MAP function
def mapper(record):
    # YOUR CODE GOES HERE
    key = record[1]
    mr.emit_intermediate(key, record)

# Implement the REDUCE function
def reducer(key,list_of_values):
    # YOUR CODE GOES HERE
    index = []
    line = []
    for v in list_of_values:
    	if v[0] != "order":
    		line.append(v)
    	else:
    		order = v
    for l in line:
    	mr.emit(order+l)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
