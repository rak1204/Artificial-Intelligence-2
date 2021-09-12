import MapReduce
import sys

"""
Assymetric Relationships in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

# Implement the MAP function
def mapper(record):
    # YOUR CODE GOES HERE
    person = record[0]
    friend = record[1]
    mr.emit_intermediate(person,friend)


# Implement the REDUCE function
def reducer(key,list_of_values):
    # YOUR CODE GOES HERE
    for person in list_of_values:
    	if person in mr.intermediate.keys():
    		if key not in mr.intermediate[person]:
    			mr.emit((person,key))
    for person in list_of_values:
    	if person not in mr.intermediate.keys():
    			mr.emit((person,key))



# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
