import MapReduce
import sys

"""
Inverted Index Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

# Implement the MAP function
def mapper(record):
    # YOUR CODE GOES HERE
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
      mr.emit_intermediate(w, key)
    
    

    

# Implement the REDUCE function
def reducer(key, list_of_values):
    # YOUR CODE GOES HERE
    total = []
    for v in list_of_values:
      if v not in total:
        total.append(v)
    mr.emit((key, total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
   inputdata = open(sys.argv[1])
   mr.execute(inputdata, mapper, reducer)
