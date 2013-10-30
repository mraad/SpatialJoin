BEGIN{
  FS=","
  OFS=","
  print "var " N "=["
}
{
    X=$2
    Y=$3
    W=$4
    H=$5
    M=X+W
    N=Y+H
    print "{\"type\":\"Polygon\",\"coordinates\":[[["X,Y"],["M,Y"],["M,N"],["X,N"],["X,Y"]]]},"
}
END{
    print "];"
}