Whitelist data source is designed to allow for Unity Catalog Volumes to work with Raster/Vector/Grid X packages. </br>
This DS does very little logic wise and it mostly there to ensure .load method is called. </br>
Hadoop Configuration for Spark and UC are properly resolved during .load calls. </br>
This DS is used to inject a .load dummy call into other DS definitions at planning of partitions. </br>
This allows for running complex spark jobs while planning partitions, very useful when dealing with large number of rasters. </br>
Note:</br>
<ul>
    <li>The actual logic occurs only in WhitelistBatch</li>
    <li>Other classes are boilerplate - there is a lot of room for improvement and bringing this to less code</li>
</ul>