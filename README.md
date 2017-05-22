# Attributions


Please open the sbt file with IDEA and import it. Then right click on AttributionCalculator.scala. 
This will write the two required output csv files. 

To make it easier to read the output (between all the debug info scrolling by), 
you can put a break point on the line that prints out the statistics, and click Debug, not Run, 
so that the app will pause there. I did not disable the debug output, because it is usefull
when an app is in development stage.

To run the tests, right click and and run AttributionCalculatorTest.

## Further steps that I did not have time for

- Git repository should not have derived files. So project files, etc, should all be ingored
- Fat jar should be built using assembly for sbt, to make it easy to do spark-sumbit.
- In a real app, this would not be development ready, until tested on a spark cluster. I hardcoded local
execution here for simplicity
- While the tests do give me a reasonable level of confidence of correctness, they should
be more extensive for a producrtion application, to make sure all the edge cases are
covered correctly, for example regarding aggregation
- Performance has not been addressed. To evaluate performance improvement opportunities for example partial result caching 
could be explored, which I don't yet do in the code. Data shuffling could be analyzed, and if a problem, 
the code would need to change to do reduce when possible on the same node. Right now I just let Spark 
manage shuffling based on its default bahvior. Also, files could be initially read into proper partitions
say on HDFS, so that Spark could start with partitioned data. Optimal ways of partitioning could be explored. 
Here again, I am just using the mechanism that Spark used by default.




