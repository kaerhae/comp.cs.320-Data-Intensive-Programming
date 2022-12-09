# Scala assignment

Contains assignment tasks from course, where task was to create ML practices with sample CSV data. 

Focus was to aim for functional programming style and form a basic ML practices using Apache Spark and MLlib.

In assignment, all basic tasks were done with also additional tasks 1, 2, 4.

## Assignment and task description

## Basic task 1: Basic 2D K-means
The task is to implement k-means clustering with Apache Spark for two-dimensional data. Example
data can be found from file data/dataD2.csv (ignore the LABEL column in this task). The task is
to compute cluster means using DataFrames and MLlib. The number of means (k) is given as a
parameter. Data for k-means algorithm should be scaled, but it is not required to scale the resulting
cluster centers back to the original scale to complete this basic task (see Additional Task #6).

## Basic task 2: Three Dimensions
The task is to implement k-means clustering with Apache Spark for three-dimensional data. Example
data can be found in file data/dataD3.csv (ignore the LABEL column in this task). The task is to
compute cluster means with DataFrames and MLlib. The number of means (k) is given as a parameter.
Remember to scale your data for the algorithm similarly to task 1.

## Basic task 3: Using Labels
K-means clustering has been used in medical research. For instance, our example data could model
some laboratory results of patients and the label could imply whether he/she has a fatal condition or
not. The labels are Fatal and Ok.
Use two-dimensional data (like in the file data/dataD2.csv), map the LABEL column to a numeric
scale, and store the resulting data frame to dataD2WithLabels variable. And then, cluster in three
dimensions (including columns a, b, and the numeric value of LABEL) and return two-dimensional
clusters means (the values corresponding to columns a and b) for those two clusters that have the
largest count of Fatal data points. You can assume that the input data frame and the total number of
clusters (k) is selected in such a way that there will always be at least 2 cluster centers which contain
Fatal data points. Remember to scale your data similarly to task 1.

## Basic task 4: Silhouette Method
The silhouette method can be used to find the optimal number of clusters in the data. Implement a
function which returns an array of (k, score) pairs, where k is the number of clusters, and score is
the silhouette score for the clustering. You can assume that the data is given in the same format as
the data for Basic task 1, i.e., two-dimensional data with columns a and b.

## Additional task 1: Functional style – 0.5 Points
Try to write your code in functional programming style. Use, for example, immutable variables, pure
functions, higher-order functions, recursion, and mapping. Avoid looping through data structures. It
is quite possible that you will achieve this task without much effort just by following the style used in
the course material.

## Additional task 2: Efficient usage of data structures – 1 Point
Use data structures efficiently. For example:
• Use caching or persisting if it is sensible.
• Consider defining schemas instead of inferring them.
• Avoid unnecessary operations.
• Adjust the amount of shuffle partitions if it is sensible. Reason in comments why or why not
to adjust the amount of shuffle partitions.

## Additional task 3: Dirty data – 1 Point
The program should handle dirty or erroneous data somehow. Add a few additional test cases using
erroneous data files. You can use the file data/dataD2_dirty.csv as an example on what a dirty
data could look like, but you can also create your own erroneous data files.

## Additional task 4: ML (Machine Learning) pipeline – 0.5 Points
Chain your ML tasks as a ML pipeline with multiple stages. (For example, VectorAssembler,
MinMaxScaler, Kmeans). To get the point from thistask you have to use ML pipelines when calculating
the results in the basic tasks.

## Additional task 5: Visualization – 1 Point
Make programmatically a graph that presents the silhouette score as a function of k (the result of
Basic task 4). You can save the graph as an image or make your application wait for a while when the
graph is visible.
With Scala the following two libraries have been tested to work with the assignment template:
• breeze-viz (version 1.3): https://github.com/scalanlp/breeze
o Spark already includes the breeze library, only breeze-viz library needs to be added
• nspl (version 0.5.0): https://github.com/pityka/nspl
With Python you can use, for example use the matplotlib library: https://pypi.org/project/matplotlib/

## Additional task 6: Scaling back to original scale - 1 Point
Scale the cluster centers back to the original scale. I.e., if the values for column X were originally in
the range [X_min, X_max], then the returned cluster centers should also be in that same range.
To get the point from this task all the basic tasks must include this feature and return the cluster
centers in the original scale.