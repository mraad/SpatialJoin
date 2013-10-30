BEGIN{
  print "{\"type\":\"FeatureCollection\",\"features\":["
}
{
  print "{\"geometry\":" $0 ",\"properties\":{\"ID\":" NR "}},"
}
END{
  print "]}"
}
