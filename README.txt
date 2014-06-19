Purpose:
=======
To compare KV, MultiKV ifile formats with different options like with/without compression, with/without RLE etc.

To run:
======
mvn clean package -DskipTests=true exec:java -Dexec.mainClass="org.apache.tez.runtime.library.common.ifile2.benchmark.Benchmark" -Dexec.args="file:////Users/...directory location.../store_sales_60_l.csv"
