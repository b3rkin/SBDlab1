# Lab 1 Report


## Usage
### Setting up the environment
After downloading the project folder several steps are required to use the application. First of all, sbt, the spark 
shell, and spark-submit should be build in a docker container within the root folder of the project exactly as is 
stated in the laboratory manual. Once this setup is completed the application should be compiled from within the sbt 
interactive shell that is started in the root folder of the project with the "compile" command. Then the "package" 
command should be used to create a ".jar" file that can be submitted to a spark cluster. The resulting ".jar" file is 
named "lab-1_2.12-1.0.jar" and is saved it the "target/scala-2.12/" directory that can be found in the root folder of 
the application. 

### Starting the application
To start the application, the ".jar" that is created can be sent to the spark cluster with the spark-submit command
executed in the root folder of the application. The Uber H3 package should be included in the command by the
"--packages 'com.uber:h3:3.7.0'" argument, and it is advised that the memory of the JVM is set to at least 8 GB by 
providing the "--driver-memory 8g" argument to the spark-submit command as well. Once the application has started It 
will prompt the following message on the command line: "Please specify a sea level rise in meters: ". Now the desired 
rise of sea level can be given as input to the program. The input must be an integer, Otherwise the program will prompt
an error message telling the user to input an integer. After the initial input is provided, the application will run 
automatically until the end. Two resulting data frames (That were required in the excellent part of the project) will
be saved as .orc files automatically in the root folder of the project in two separate folders named: 
"output_excellent_orc", which contains all cities and destinations, and "output_excellent_population_orc" which contains
the old and new populations of the destinations. The program will also automatically read these .orc files into two spark
data frames and show their respective output on the terminal. One important note is that the generated output files 
should not already exist in the root folder of the project, otherwise the program will prompt that the output already 
exists, and will not calculate a new output


## Functional overview

Some introduction and general description is needed here maybe
### User input and Spark environment 
The initial step  of the application is to obtain the sea level rise as input from the user. The input is taken as an 
integer in meters. The type of input is guaranteed to be an integer and the program will prompt for an integer until 
one is provided. This is done with a try and catch exception structure. The input is assigned to a variable named 
"sea_level_rise" and is obtained with a global function that is called "setup". This function also suppresses the
verbose logging of Spark. After the "setup" function is completed, a spark session is created and built. The spark 
master is set to local in the current state of the application and should be altered if a remote cluster is to be used.

### Loading the data sets into Spark
The application uses two data sets to obtain the desired output. The first data set is the data set that contains the
map of the Netherlands from the OpenStreetMaps project. This data set is loaded from an ".orc" file called 
"netherlands.orc" into a dataframe in the application called "df_raw". From this dataframe only the necessary columns
are selected with ".select()" into a dataframe called "df_OSM". The selected columns are "id", "type", "tags", "lat",
and "lon". The second data set that is loaded into the application is the data set that contains elevation data of the 
Netherlands from the ALOS project. The data is loaded into a dataframe called "df_ALOS" from the provided parquet files.

### Joining the OSM and ALOS dataframes
The dataframes that are loaded in the application, "df_OSM" & "df_ALOS", are joined using the "joinOnH3Index" function. 
This function adds a new column to both data frames called "H3". The column contains the h3 indices for the corresponding
coordinates in each row of the dataframes. The new column and indices are added with the ".withColumn()" operation using 
the "lat" and 
"lon" columns. within the ".withColumn()" a user defined function, udf, is used to be able to use the "geoToH3" function 
of the Uber H3 package. This udf is defined in the global scope with an object named "H3" and is serialized using the 
generic Java serializer class: "Serializable". Serialization is required for the ".withColumn()" operation as the third
party function of the h3 package is outside the Spark environment. After the h3 
indices are added to the "df_ALOS" data frame multiple coordinates will have the same H3 index since they fall inside 
the same h3 hexagonal tile. This is illustrated in Fig. 1. The coordinates that fall in the same tile will have the 
same index. In order to determine the elevation that each tile will 
represent, the average of the elevation of all the points that fall inside each tile is taken. This is done via the 
".groupBy()" method 
on the ALOS dataframe and with using the ".avg()" built-in function of Spark. ".groupBy()"  is done on the newly added
"H3" column such that the "df_ALOS" dataframe now represents the h3 hexagonal tiles and their elevation. After this 
operation, all tiles that cover
the Netherlands will have a distinct elevation assigned to them. This will make it easier to join the ALOS dataframe 
with the OSM dataframe. The reason behind this is that an elevation can now be assigned to a place according to in which
h3 hexagonal tile the place is located. This can now be done with an inner join instead of a cross join.The final step 
of the "joinOnH3Index" function is indeed joining the dataframes 
by their "H3" 
column. The ".join()" inner join is used hence, all places in the OSM dataframe now have an assigned elevation. As 
mentioned before the "geoToH3" function from 
the h3 package is used to obtain h3 indices. This function is placed in the before mentioned udf. 
The resolution for the h3 indexing is chosen to be 10. The reason for this 
is that each tile covers approximately 15k square meters. Approximately 10 ALOS data points fall in this area which when 
averaged can give us a rough estimation of the elevation of the general landscape. Taking the average as elevation may 
vary over each city but will determine roughly if it is safe after a flood of a certain size. When compared with actual
elevations of certain cities the results have proven sufficiently accurate. Some examples can be seen in Table 1.<br><br>
Table 1:
 <table>
  <tr>
    <th>City</th>
    <th>Actual Elevation (m)</th>
    <th>Calculated Elevation (m)</th>
  </tr>
  <tr>
    <td>Heerlen</td>
    <td>113</td>
    <td>122.8</td>
  </tr>
  <tr>
    <td>Amersfoort</td>
    <td>3</td>
    <td>8.05</td>
  </tr>
<tr>
    <td>Dordrecht</td>
    <td>1</td>
    <td>1.95</td>
  </tr>
</table> 
<br>



![Alt text](images/lab1img1.png?raw=true "Figure ")<br>

<p style="text-align:center;">
    Figure 1: Representation of H3 tiles on the ALOS data points
</p>


### Identifying flooded places and safe destinations
The joined dataframe that now contains elevation data is stored in "df_OSM_ALOS". A filter operation on the newly added
elevation data is applied to split 
the dataframe into two separate dataframes that represent places under and above water: "df_underWater", and 
"df_aboveWater". The filter is applied with the previously obtained user input: "sea_level_rise". The next step is to 
configure the dataframes for further manipulation. This is done with the defined "updateDfUnderWater", and "updateDfAboveWater"
functions. Both these functions operate in the same way. Places, names, and population are extracted from the tags column. 
For "df_underWater" only cities, hamlets, villages and towns are selected, for "df_aboveWater" only cities are selected.
The unnecessary columns are dropped. Furthermore, the "lat" and "lon" columns are renamed as "lat_OSM_Dest" and "lon_OSM_Dest"
for "df_aboveWater" as these will be destination cities. The same columns are renamed "lat_OSM" and "lon_OSM" for 
"df_underWater". Now the clean dataframes with only useful information is obtained for the places that need evacuation
and the cities that will act as destinations. Besides the destination cities, also the destination harbours are obtained
from "df_aboveWater" and a new data frame is formed with the 
"updateDFHarbour" function such that it contains all harbours that are not flooded. (Note that, in our implementation, we 
considered only harbours that are not flooded as safe harbours that can evacuate people to Waterworld. To change this and 
include all possible harbours in the country, the dataframe inside the "updateDFHarbour" should be switched simply to 
"df_OSM_ALOS" instead of "df_aboveWater"). As a next step, for the places that have been 
flooded, the closest safe city and the closest safe harbour is obtained in two separate data frames with the defined
"findClosestHarbour", and "findClosestCity" functions. The obtained dataframes from these functions are called 
"evacuationPlanHarbour", and "evacuationPlanCity" respectively. Both functions operate in a similar way. First an inner join 
operation 
is done between "df_underWater" and the destination dataframe: "df_aboveWater" for cities and "df_harbours" for harbours.
Since we are considering only the Netherlands, the 
destination dataframe is small, max 30 entries
for df_aboveWater. Therefore, the join is a broadcast hash join. This way only relevant parts of the joined data frame are 
broadcast to the partitions that contain the first dataframe. The broadcast should be removed when the initial datasets 
are scaled up. After this join, the distance between the destinations and the 
flooded places is calculated via their respective coordinates with the following equation: 
D^2 = (lat1-lat2)^2 + (lon1-lon2)^2 where D is the distance. A ".withColumn()" is used. The distances are added to a 
separate column of the joined dataframes. Furthermore, 
since in the functions the distance between each place and each destination is calculated, only the smallest distance is
obtained by "groupBy()" and "min" operations. The grouping is done on the column "Name" and min is taken of the 
"distance" column
so each row now contains a distinct place with the closest safe city for "evacuationPlanCity" and the closest safe 
harbour for "evacuationPlanHarbour". Now two dataframes that contain the flooded places with their distances to 
the nearest safe city and nearest safe harbour is obtained in the "evacuationPlanHarbour", and "evacuationPlanCity" 
dataframes

### Preparing the list of destination cities
As a requirement, the application should have an output list that contains the safe destination cities including 
Waterworld with information about their initial populations and their population after the evacuations have 
taken place. In this step this list is initialized. First a dataframe is created from the "spark" object that contains 
two columns named "Population_aboveWater" and "Dest_cities" and a single row entry that contains 0 and "Waterworld". 
This dataframe is called "waterworldEntry". This way the old population of Waterworld will be set to 0. 
The next step is to select only the "Population_aboveWater" and "Dest_cities" columns from the "evacuationPlanCity"
dataframe into a new dataframe and adding the "waterworldEntry" with ".union()" Now a dataframe, "old_population" is 
formed with two column containing population information and the names of the safe destination cities including 
Waterworld.

### Obtaining the final results
In this final part of the application the final dataframes are obtained. There should be two resulting dataframes. The 
first dataframe should contain a list of places: cities, towns, villages, and hamlets that have been flooded by the 
amount of sea level rise that is specified at the start of the application by the user and the number of evacuees from 
those places to either safe cities or Waterworld. If a harbour is closer than a safe city, then 25% of the population of
that place should evacuate to Waterworld, while 75% should evacuate to the closest safe city. And otherwise 100% of the 
population should evacuate to the closest safe city. The second output dataframe should contain a list of destination 
cities and Waterworld with their initial population and the population after the evacuations. The last dataframes that 
the application has formed were the "evacuationPlanHarbour" and "evacuationPlanCity" data frames. These dataframes both
contain a list of all flooded places with their closest safe city or harbour. Now it should be determined if a city is 
closer or a harbour. This is done with the defined "isHarbourOrCityCloser" function. Both the dataframes are passed as 
arguments to this function. Within this function first an inner join by the "Name" columns is applied into a temporary
dataframe called "df_result". This data frame now contains all flooded places and in two separate columns the distances 
to both the closest safe city and the closest safe harbour. Next, a "withColumn()" is used to add a new column called
"isCityCloser" with a boolean value which is true if the safe city distance is smaller than the harbour distance. This
boolean value is obtained withing the "withColumn()" by simply comparing the city and harbour distance column with the 
">" operator. Next, another "withColumn()" is used to obtain a new column called "updated_dest_cities". The values of 
this column are determined by the newly formed "isCityCloser" column. If it is true, that row will get the safe city for
this "updated_dest_cities" column and if false, it will get "Waterworld". This dataframe is the output of the 
"isHarbourOrCityCloser" function and is placed in a dataframe named "x". The next step is to split the populations that 
will form the number of evacuees of each place if a harbour is closer. The defined "splitPopulation" function is used 
for this purpose. This function simply adds two new columns to the input dataframe that contain 25% and 75% of the 
"population_underWater" column. This is achieved with the ".withColumn()" function. Now, the "x" dataframe contains all 
the information that needs to be known in order to device the ultimate evacuation plan. The dataframe must be 
manipulated in such a way that the desired output dataframe is obtained. Towards this goal, three separate dataframes 
are formed from "x": "waterWorldDestination25",  "waterWorldDestination75", "otherCityDestination". x is filtered on 
the "updated_dest_cities" column that was formed in the "isHarbourOrCityCloser" function for all three dataframes. for 
"waterWorldDestination25" and "waterWorldDestination75", only entries that contain "Waterworld" in their
"updated_dest_cities" columns are chosen. These are the places for which the populations must be split. Previously two 
columns with the split populations were added to "x" . Hence, for "waterWorldDestination25", after the filter 
operation the following columns are selected: "Name", "population_25", "dest_cities". Similarly, for 
"waterWorldDestination75" the same columns are selected but with "population_75" instead. Now the "otherCityDestination"
dataframe remains. This dataframe is formed from "x" by filtering for each element for which the "updated_dest_cities" 
column is not waterworld. The population of these cities will entirely be evacuated to the nearest safe city. Again the
"Name", "Population_underWater", "updated_dest_cities" columns are selected. Each of the three formed dataframes now 
contain three columns with their name, the number of evacuees and their destination. For each of the three dataframes 
the columns are renamed with the ".withColumnRenamed" function to "place", "num_evacuees", and "destination"  as is 
required in the manual. The final step is to combine the three formed dataframes into a single data frame. This is done
with the ".union()" function and the resulting dataset is placed into a dataframe named "result". Now the first result 
dataframe is obtained, the list with destinations must be obtained as well. For this purpose the "place" column is 
dropped from "result" and is joined with the formerly initialized "old_population" dataframe. This is an inner join on 
the "destination" column of "result". The resulting dataframe is named "y". This contains the "result" dataframe with
added columns that specify the old population of their destination cities or 0 in case its Waterworld. The next step is
to do a ".groupBy()"  function on the destinations and the ".sum()" function. With this operation the sum of all 
evacuees to each destination city is obtained in distinct rows in the dataframe. The last step is to inner join this 
obtained dataframe with "y" on the "destination" column such that now the dataframe contains each distinct destination
with its old population and number of evacuees to that destination. As is required the new population should be in the 
list instead of the number of evacuees so a ".withColumn()" is done to create a "new_population" column by adding the 
existing two columns of total number of evacuees and old population. The unnecessary columns are dropped and the final 
dataframe is obtained in a dataframe named "z". This dataframe now contains a list of destination cities with their old
and new populations.

### Generating output files and testing the application
The desired dataframes are obtained in the application. As a requirement these dataframes should be saved as .orc files
on the system. this is achieved by passing the "result" dataframe to the defined "ouputOrc" function and the "z" 
dataframe to the "outputOrc_2" function. "ouputOrc" checks if the output is already existing. If so a message is 
displayed as follows: ".orc files already exist" and the dataframe cannot be saved to the system. If it does not already
exist the .orc files are written into a folder named "output_excellent_orc". Finally, it will print the sum of all 
evacuees to terminal by doing a ".sum()" function on the "num_evacuees" column and extracting the first entry. 
"outputOrc_2" operates similar. This output .orc file of "z" is written inside the "output_excellent_population" folder. 
These folders can be found in the root folder of the application after execution. Again, an output must not already 
exist for the application to be able to save the outputs in a .orc file. For visually assessing the operation of the 
application in the end, the saved output files are loaded from the .orc files and shown on the terminal. Finally, the
spark context is stopped with a "spark.stop" command. 


## Result
There are in total three distinct outputs of this application. They are dependent on the initial input of the program 
which is sea level rise. The first output is the total number of evacuees, the
second output is a dataframe that contains the names of places and the number of evacuees of these places with their 
destinations, either Waterworld or a safe city. Lastly, the third output is a list with safe cities including Waterworld
, containing their old and new population after the evacuation process. The program has been executed with three 
different input for the sake of this report, which are 0m, 10m and 100m. The first and third output is presented for 
these inputs and a fraction of their second output is shown as well. </br>


Sea Level Rise = 0m:</br>


Sum of all evacuees: 1196910 </br>

Data Frame 1:
<pre>
+--------------------+------------+-----------+
|               place|num_evacuees|destination|
+--------------------+------------+-----------+
|        Rijpwetering|        1595|     Leiden|
|Nieuwerkerk aan d...|       21595|  Rotterdam|
|          Woldendorp|         915|  Groningen|
|Capelle aan den I...|       66421|  Rotterdam|
|         Zuiderwoude|         300|  Amsterdam|
|          Slappeterp|          88| Leeuwarden|
|Ouderkerk aan de ...|        8620|  Amsterdam|
|            Middelie|         705|  Amsterdam|
|           Oosteinde|         160|  Groningen|
|           Snikzwaag|          65| Leeuwarden|
|   Woerdense Verlaat|         828|  Amsterdam|
|      Nieuwolda-Oost|         110|  Groningen|
| Berkel en Rodenrijs|       30048|  Rotterdam|
|     Sibrandabuorren|         365| Leeuwarden|
|Krimpen aan den I...|       29123|  Rotterdam|
|                 ...|         ...|        ...|
|                 ...|         ...|        ...|

</pre>
Dara Frame 2:


<pre>
+-----------+--------------+--------------+
|destination|old_population|new_population|
+-----------+--------------+--------------+
|  Amsterdam|        841282|      914548.0|
|  Dordrecht|        118703|      165413.0|
|    Tilburg|        199128|      199456.0|
| Amersfoort|        139259|      347670.0|
|  Apeldoorn|        141107|      145803.0|
|  Rotterdam|        572392|      763800.0|
|  Groningen|        201242|      242606.0|
| Zoetermeer|        124780|      139084.0|
| Leeuwarden|         91992|      120203.0|
| Middelburg|         40345|       48020.0|
|    Alkmaar|         96460|      256745.0|
|    Utrecht|        295591|      312956.0|
|    Haarlem|        158593|      306344.0|
|      Delft|        101386|      204604.0|
| Waterworld|             0|      108570.0|
|     Leiden|        123753|      138322.0|
|     Zwolle|        124505|      137554.0|
|      Emmen|         56113|       71843.0|
+-----------+--------------+--------------+
</pre>


Sea Level Rise = 10m:</br>


Sum of all evacuees: 9490091 </br>

Data Frame 1:
<pre>
+--------------------+------------+----------------+
|               place|num_evacuees|     destination|
+--------------------+------------+----------------+
|          Exloerveen|          97|           Emmen|
|          IJzendijke|        2284|      Middelburg|
|         Kudelstaart|        9218|         Haarlem|
|Tienhoven aan de Lek|         780|         Utrecht|
|          Overschild|         524|       Groningen|
|     Sellingerbeetse|         200|           Emmen|
|               Oijen|        1175|'s-Hertogenbosch|
| Alphen aan den Rijn|       71428|         Haarlem|
|              Bergen|       12311|         Haarlem|
|Nieuwerbrug aan d...|        1756|       Amsterdam|
|Sint Anna ter Muiden|          55|      Middelburg|
|  Krimpen aan de Lek|        6580|           Breda|
|        Friese Buurt|          90|         Haarlem|
|             Monster|       13900|         Haarlem|
|              Koudum|        2695|       Amsterdam|
|                 ...|         ...|             ...|
|                 ...|         ...|             ...|


</pre>
Dara Frame 2:


<pre>
+----------------+--------------+--------------+
|     destination|old_population|new_population|
+----------------+--------------+--------------+
|       Amsterdam|        841282|     1865058.0|
|        Nijmegen|        176731|      188950.0|
|         Tilburg|        199128|      345370.0|
|        Deventer|         81505|      472031.0|
|             Ede|         72460|      392878.0|
|'s-Hertogenbosch|        115903|      385580.0|
|       Apeldoorn|        141107|      438400.0|
|       Groningen|        201242|      774073.0|
|      Middelburg|         40345|      494993.0|
|        Enschede|        148874|      152029.0|
|         Utrecht|        295591|     1509516.0|
|         Haarlem|        158593|     2625724.0|
|           Breda|        150008|     2067857.0|
|      Waterworld|             0|        2410.0|
|          Arnhem|        155694|      158501.0|
|           Assen|         66895|      320836.0|
|           Emmen|         56113|      197356.0|
+----------------+--------------+--------------+
</pre>


Sea Level Rise = 100m:</br>


Sum of all evacuees: 16565460 </br>

Data Frame 1:
<pre>
+--------------------+------------+-----------+
|               place|num_evacuees|destination|
+--------------------+------------+-----------+
|         Einighausen|        1279|    Heerlen|
|               Bunde|        5707|    Heerlen|
|      Son en Breugel|       16626|    Heerlen|
|        Geldermalsen|       10535|    Heerlen|
|               Corle|         263|    Heerlen|
|         Hoog Soeren|         342|    Heerlen|
|           Kruishaar|         300|    Heerlen|
|        Hoogblokland|        1400|    Heerlen|
|           Westbroek|        1187|    Heerlen|
|         Daarlerveen|        1151|    Heerlen|
|     Drogteropslagen|         530|    Heerlen|
| Berkel en Rodenrijs|       30048|    Heerlen|
|             Peperga|          87|    Heerlen|
|             Blokker|        3810|    Heerlen|
|           Dwingeloo|        2740|    Heerlen|
|                 ...|         ...|        ...|
|                 ...|         ...|        ...|

</pre>
Dara Frame 2:


<pre>
+-----------+--------------+--------------+
|destination|old_population|new_population|
+-----------+--------------+--------------+
|    Heerlen|         67943|   1.6633403E7|
+-----------+--------------+--------------+
</pre>

When sea level rise is 0 meters, normally it would be expected that no evacuation is needed, yet the application still
counts more than 1 million evacuees. The main reason for this is that especially near the coast, many locations in the 
Netherlands lie below sea level. These places are protected by mostly man made and some natural barriers. These
barriers are not taken into account by our application so these places are listed for the evacuation plan. From output
data frame 1 it can be seen that the places consist mainly of coastal villages, e.g. Woldendorp, Oosteinde: north coast
or Zuiderwoude: Coast of Markermeer. It can also be observed from the results that the evacuees to Waterworld decrease 
as the sea level rise increases. The reason behind this is that in our application, only safe harbours are taken into 
account when evacuation to Waterworld is considered. As sea level increases, fewer safe harbours will remain. To take all
harbours into account the change described in section "Identifying flooded places and safe destinations" must be made.
The only large city in the netherlands is Heerlen with an elevation of 122 meters. Therefore, when sea level rise is
selected as 100 meters, the only remaining safe city is Heerlen. No waterworld evacuees can be observed as all harbours 
will be flooded at this sea level rise. The total number of evacuees is roughly 16.5 million at this elevation which
corresponds to a little outdated population of all the netherlands. When the old population of Heerlen is subtracted
form its new population, the same number of evacuees arise.

## Scalability
In order to conserve the scalability of our program, we have tried to maintain the complete application within the 
spark context and have mainly utilized Spark SQL with Spark DataFrames. In this way we can ensure that when the 
application is deployed to a Spark cluster that has multiple 
Spark workers, it can efficiently continue processing without any performance issues. The first optimization that we 
have included in this sense is to switch from user defined function for certain calculations in the application, to 
built-in functions of Spark. the distance calculations and the comparison algorithms are such parts of the program that
we have changed. The one user defined function (udf) that remains in the program is the function used to calculate h3 
indices based off of coordinates. This is a third party function, and therefore it needs to be serialized and deserialized 
each time it is called. Although this might hinder the scalability of the application, this step is needed nonetheless. 
Another component of the program that relates to scalability are the many join operations performed on the data frames. 
Formerly we had implemented an approach with reducing data frames after cross-joining them. since cross-joining will 
produce an inner product of the dataframes, for larger data frames such an approach could yield major performance 
issues. Instead of performing cross-joins, now we reduce the data frames first and then perform an inner join operation
on the data frames which result in the same number or fewer rows as the operand data frames. One component of the program
that might pose issues in scalability is are the broadcast of dataframes during the join operations in the 
"findClosestCity" and "findClosestHarbour" functions. These broadcast should be eliminated when the input data sets of 
the program are scaled up. In general the application remains in the Spark context and utilizes Spark SQL operations 
which ensure good scalability.




## Performance
### Performance profiling background

In order to profile the performance of this application, the program has been run twice for three different user inputs
and the total run-time is taken as the average. 
These inputs are 0 meters, 10 meters, and 100 meters sea level rise. The application is submitted each time to a local 
spark cluster and execution times are taken from the Spark history server. The hardware on which these performance benchmarks
are run are as the following:

<pre>
CPU: Intel® Core™ i7-7700HQ CPU @ 2.80GHz × 8 
RAM: 15.5 GiB
OS: Ubuntu 20.04.3 LTS, 64-bit, GNOME version: 3.36.8
</pre>

The memory is set to 8 GiB for the executions by supplying the "--driver-memory 8g" argument to the spark-submit command.
The result of the general execution time benchmark can be seen in Table 2:

Table 2:
 <table>
  <tr>
    <th>User input (m)</th>
    <th> Run time (min)</th>

  </tr>
  <tr>
    <td>0</td>
    <td>9.8</td>
  </tr>
  <tr>
    <td>10</td>
    <td>9.7</td>
  </tr>
<tr>
    <td>100</td>
    <td>9.7</td>
  </tr>
</table> 
<br>

The results obtained in Table. 2 indicate that user input has no significant effect on the performance of the application.
With this in mind the following performance observations will be based on the average run of the application twice for a
user input of 10 meters sea level rise.

### Performance of the application

The application runs with a total of 27 Spark jobs (Many repetitive). Jobs indicate each time an action takes place in the program which
triggers the actual calculation of the output via the Directed Acyclic Graph (DAG). Since certain operations on Spark
dataframes are lazily evaluated, computation takes place whenever the actual output is needed. The first job in the 
program is the loading of data from the parquet files for the ALOS data set. The files are loaded and parallelized into
a data frame. This action takes 0.5 seconds


Another job is triggered by the ".IsEmpty()" function that checks if the data frame that will be joined in the 
"isHarbourOrCityCloser" function is empty or not. on average, it takes 0.1 seconds for its many calls

One part of the application that takes a significant amount of time is the adding h3 indices to the two dataframes 
"df_OSM" and "df_ALOS", and the joining the data frames. Adding the H3 indices takes 3.8 minutes for each data frame
and is happening in parallel. This is an expected long operation since the function,
used from the h3 package should be serialized and deserialized each time it is called. Taking into account that the 
dataframes are also very large at this point in the program it is an expected result.

Another part of the program that has a large execution time happens during the "findClosestHarbour" and 
"findClosestCity" functions. In these functions two dataframes are joined together with a broadcast hash join, which 
means the second dataframe is broadcast to each partition separately. The duration of these operations is 3.6 and 3.7 
minutes. As run in parallel the actual duration is 3.7 minutes. 

The application contains several other tiny jobs that take between a millisecond and 47 seconds. These include the 
reading of the .orc files at the very beginning and in the end of the application. As well as showing the resulting
data frames with a ".show()" function.

One significant optimization in terms of run-time performance is achieved during the "isHarbourOrCityCloser" function
which includes a join operation on two dataframes which is normally expected to have a long execution duration. This 
optimization is achieved by persisting the two dataframes that are the arguments of this function with the ".cache()"
function. As the two dataframes are persisted in memory, the dataframes do not need to be calculated from their DAGs and
can be utilized directly.