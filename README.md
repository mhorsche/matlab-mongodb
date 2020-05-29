# MATLAB® Interface for MongoDB

This is a MATLAB® interface that implements basic functionallity for MongoDB using the  `mongo-java-driver` (https://github.com/mongodb/mongo-java-driver).

This library requires:
* MATLAB release R2017b or later
* Database Toolbox (for [MongoDB API](https://de.mathworks.com/help/database/ug/mongo.html))

The original MongoDB API provided by MathWorks is implemented in the [Microsoft® Azure Cosmos DB™](https://github.com/mathworks-ref-arch/matlab-azure-cosmos-db)

## Getting started

To install the *MongoDB API* be sure the `mongo-java-driver` is added to [MATLAB® Java class path](https://de.mathworks.com/help/matlab/matlab_external/java-class-path.html) variables.

```java
javaaddpath('mongo-java-driver-3.12.0.jar','-end');

javaclasspath('-dynamic')

  DYNAMIC JAVA PATH

MongoDriver/mongo-java-driver-3.12.0.jar 
```

You should now be able to use the `mongo` command to open a connection. In addition to the MathWorks version the `aggregation` pipeline command was implemented to allow complex queries. Since the implementation is not tested, there is no guarantee for correct functionality.

## MongoDB API
The following sample code connects to a database and inserts some sample data.

```java
%% Create connection to Cosmos DB Mongo API
% Use valid values for localhost, testdb, myusername & mypassword
conn = mongo("localhost",27017,"testdb","UserName","<myusername>","Password","<mypassword>");

% Create a collection
myCollectionName = 'product';
conn.createCollection(myCollectionName);

% Check if there are anything in the collection already, there should not be
dataCount = count(conn, myCollectionName);

% Create some sample data
[insertcount, documents] = insert(mongodbconn,myCollectionName,'{"key1":"value1","key2":"value2"}')
[insertcount, documents] = insert(mongodbconn,myCollectionName,'{"key1":"value1","key2":"value3"}')

% Run some aggregation pipeline
result = aggregate(mongodbconn,myCollectionName,'[{"$group": {"_id": "$key1", "count": {"$sum": 1}}}]')

% Close the connection when finished
conn.close();
```


## License (provided by MathWorks)
The license for the MATLAB Interface for *MongoDB API* is available in the [LICENSE.md](LICENSE.txt) file in this GitHub repository. This package uses certain third-party content which is licensed under separate license agreements. See the [thirdparty_package_registry.xml](registry/thirdparty_package_registry.xml) file for third-party software.