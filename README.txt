Purpose:
=======
To compare KV, MultiKV ifile formats with different options like with/without compression, with/without RLE etc.

Ref: https://issues.apache.org/jira/browse/TEZ-1228

The current vertex-intermediate format used all across Tez is a flat file of variable length k,v pairs. For a significant number of use-cases, in particular the sorted output phase, a large number of consecutive identical keys are found within the same stream. The IFile format ends up writing each key out fully into the stream to generate (K,V) pairs instead of ordering it into a more efficient K,
{V1, .. Vn}
list.
This duplication of key data needs larger buffers to hold in memory and requires comparison between keys known to be identical while doing a merge sort.
This bug tracks the building of a prototype IFile format which is optimized for lower uncompressed sizes within memory buffers and less compute intensive to perform merge sorts during the reducer phase.

To run:
======
mvn clean package -DskipTests=true exec:java -Dexec.mainClass="org.apache.tez.runtime.library.common.ifile2.benchmark.Benchmark" -Dexec.args="file:////Users/...directory location.../store_sales_60_l.csv"
