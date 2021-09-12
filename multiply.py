import MapReduce
import sys

"""
Matrix Multiply in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

# Implement the MAP function
def mapper(record):
    # YOUR CODE GOES HERE
    for x in range(5):
    	if record[0] == 'a':
    		mr.emit_intermediate((record[1],x),(record[2],record[3]))
    	if record[0] == 'b':
    		mr.emit_intermediate((x,record[2]),(record[1],record[3]))




# Implement the REDUCE function
def reducer(key,list_of_values):
    # YOUR CODE GOES HERE
    lista = {}
    listb = {}
    for v in list_of_values:
    	if v[0] not in lista:
    		lista[v[0]] = v[1]
    	if v[0] in lista:
    		listb[v[0]] = v[1]

    prod = 0
    result = 0
    for x in listb.keys():
    	prod = listb[x] * lista[x]
    	result+= prod
    mr.emit((key[0],key[1],result))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
