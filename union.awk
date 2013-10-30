BEGIN{
  print "var union=["
}
{
  print $0 ","
}
END{
  print "];"
}
