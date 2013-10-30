hadoop fs -rm -skipTrash data1.txt data2.txt
awk -f data.awk | hadoop fs -put - data1.txt
awk -f data.awk | hadoop fs -put - data2.txt

time hadoop jar target/SpatialJoin-1.0-SNAPSHOT-job.jar -D com.esri.size=5 data1.txt data2.txt output

hadoop fs -cat data1.txt | awk -f geojson.awk -v N=data1 > data1.js
hadoop fs -cat data2.txt | awk -f geojson.awk -v N=data2 > data2.js

hadoop fs -cat output/part-* | awk -f union.awk > union.js
