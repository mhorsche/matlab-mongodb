classdef mongo < handle & matlab.mixin.CustomDisplay
% MONGO connect to a mongo database
%
%   CONNECT = MONGO(SERVER,PORT,DATABASE)
%   returns Mongo database object
%
%   CONNECT = MONGO(SERVER,PORT,DATABASENAME,'NAME','VALUE')
%   returns Mongo database object with additional name-value arguments. For
%   instance, you can specify UserName and Password required for
%   authentication against the database.
%
%   Input Arguments:
%   ----------------
%   SERVER   - List of servers hosting Mongo database. 
%   PORT     - List of port-numbers associated to each server.
%   DATABASE - Mongo database name to connect to.
%
%   Name-Value arguments:                                       
%   ---------------------
%   UserName       - username required to authorize user.
%   Password       - password required to authenticate a username.
%
%   Methods:
%   --------
%
%   createCollection - Create a Mongo Collection
%   dropCollection   - Drop a Mongo Collection
%   count            - Count total documents in a collection or for a Mongo
%                      Query
%   distinct         - Get distinct values in a field of documents in a
%                      collection or for a Mongo Query
%   find             - Find and retrieve documents in a collection or for a
%                      Mongo Query
%   insert           - Insert data in MATLAB as documents in a Mongo
%                      collection
%   update           - Update data in documents stored in a Mongo
%                      collection
%   remove           - Remove documents from a Mongo collection
%   isopen           - Check if connection to Mongo database is valid
%   close            - Close conection to Mongo database
%
%   Example:
%   --------
%   conn = mongo("localhost",27017,"testdb")
%
%   conn = mongo("localhost",27017,"testdb","UserName","testdbuser","Password","pass1")
%
%   conn = mongo(["localhost" "remotehost"],[27017,27018],"testdb","UserName","testdbuser","Password","pass1")
%
% Copyright 2017 The MathWorks, Inc.


  properties(SetAccess = private)

    % Stores the name of the database provided
    Database;
    
    % Stores the username used to authenticate 
    UserName;

    % Stores the name of the Server(s) to which connection is established
    Server;
    
    Port; % 0 or more MongoDB Server Port
    
    % List of Collections present in the database
    CollectionNames;
    
    % Total number of Documents across all Collections in database
    TotalDocuments;
        
  end

  properties(Access = private, Hidden = true)

    % Stores handle to 'DB' object of Mongo-JAVA driver
    DatabaseHandle;
    
    % Stores handle to 'Mongo' object of Mongo-JAVA driver
    ConnectionHandle;
    
    
  end
  
  methods(Static,Hidden=true,Access=private)
      function errmessage = extractExceptionMessage(e)
        if strcmpi(e.identifier,'MATLAB:Java:GenericException')
            exceptionObj = e.ExceptionObject;
            errmessage = char(exceptionObj.getMessage);
        else            
            errmessage = e.message;
        end
      end
  end
  
  methods(Access = public, Hidden = true)
    
    % Returns connection handle
    function connHandle = getConnHandle(mongodbconn)
      connHandle = mongodbconn.ConnectionHandle;
    end
    
    % Returns database handle
    function dbHandle = getDBHandle(mongodbconn)
      dbHandle = mongodbconn.DatabaseHandle;
    end
    
    
  end
  
  methods
      
      function collectionnames = get.CollectionNames(mongodbconn)
           
          collectionnames = {};
              try

                % Invoke Mongo-JAVA Driver API to obtain list of collections
                colllist = mongodbconn.DatabaseHandle.getCollectionNames;

                % Manually iterate through the list and build a CELL ARRAY
                iter = colllist.iterator;

                while (iter.hasNext)
                  collectionnames = [collectionnames; iter.next()];
                end

                collectionnames = collectionnames';

              catch

                collectionnames = {};

              end
                
      end
        
      function totaldocuments = get.TotalDocuments(mongodbconn)
           
              try
                totaldocuments = mongodbconn.DatabaseHandle.getStats.get('objects');
              catch
                totaldocuments = [];
              end
                
      end
      
  end
  
  methods(Access = public, Hidden = true)

    function mongodbconn = mongo(server,port,database,varargin)

          
          if nargin == 0
              return;
          end
          
          if (exist('com.mongodb.MongoCredential','class') ~= 8 || ...
              exist('com.mongodb.MongoClient','class') ~= 8 || ...
              exist('com.mongodb.MongoClientOptions','class') ~= 8 || ...
              exist('com.mongodb.WriteConcern','class') ~= 8 || ...
              exist('com.mongodb.ReadPreference','class') ~= 8 || ...
              exist('com.mongodb.ServerAddress','class') ~= 8)
                
                error(message('mongodb:mongodb:driverNotFound'));
          end
          
          import org.apache.log4j.*;

          Logger.getRootLogger().setLevel(Level.OFF);
          
          % Input parser to input validation
          p = inputParser;

          p.addRequired("server",@(x)validateattributes(x,["string" "char" "cell"],{}))
          p.addRequired("port",@(x)validateattributes(x,"numeric",{"row","nonnegative","nonnan","positive","finite"}));
          p.addRequired("database",@(x)validateattributes(x,["string" "char"],{"scalartext"}));
          p.addParameter("UserName","",@(x)validateattributes(x,["string" "char"],{"scalartext"}));
          p.addParameter("Password","",@(x)validateattributes(x,["string" "char"],{"scalartext"}));
          p.addParameter("AuthMechanism","",@(x)validateattributes(x,["string" "char"],{"scalartext"}));
          p.addParameter("ConnectionTimeout",10000,@(x)validateattributes(x,"numeric",{"scalar","nonnan","nonnegative","positive","finite"}));
          p.addParameter("ServerSelectionTimeout",10000,@(x)validateattributes(x,"numeric",{"scalar","nonnan","nonnegative","positive","finite"}));
          p.addParameter("ReadPreference","",@(x)validateattributes(x,["string" "char"],{"scalartext"}));
          p.addParameter("WriteConcern","",@(x)validateattributes(x,["string" "char"],{"scalartext"}));
          p.addParameter("SSLEnabled", "", @(x)validateattributes(x, {'logical'}, {}));
          
          p.parse(server,port,database,varargin{:});

          server = p.Results.server;
          
          if isempty(server)
              error(message('mongodb:mongodb:IncorrectDataType','server'));
          end
          
          switch class(server)
              
              case {'string','char'}
                  
                  server = cellstr(server);
                  if any(cellfun(@isempty, server) == 1)
                      error(message('mongodb:mongodb:IncorrectDataType','server'));      
                  end
                  
              case {'cell'}
                  
                  if ~all(cellfun(@ischar, server) == 1) || any(cellfun(@isempty, server) == 1)
                      error(message('mongodb:mongodb:IncorrectDataType','server'));      
                  end
                  server = cellstr(p.Results.server);
          end
          
          
          port = p.Results.port;
          databasename = char(p.Results.database);
          
          if isempty(databasename)
              error(message('mongodb:mongodb:ExpectedNonempty','database'));
          end
          
          username = char(p.Results.UserName);
          password = char(p.Results.Password);
          authmechanism = char(p.Results.AuthMechanism);
          connectiontimeout = p.Results.ConnectionTimeout;
          serverselectiontimeout = p.Results.ServerSelectionTimeout;
          readpreference = char(p.Results.ReadPreference);
          writeconcern = char(p.Results.WriteConcern);
          sslEnabled = logical(p.Results.SSLEnabled);
          
          % Length of SERVER and PORT CELL array should match, else, PORT
          % should be of length 1
          if length(port) ~= length(server)
            error(message('mongodb:mongodb:PortServerSizeMismatch'))
          end
          
          if ~isempty(password) && isempty(username)
              password = "";
          end
          
          if ~isempty(readpreference)
              readpreference = validatestring(readpreference,{'nearest','primary','secondary','primarypreferred','secondarypreferred'});
          end

          if ~isempty(writeconcern)
              writeconcern = validatestring(writeconcern,{'acknowledged','majority','w1','w2','w3'});
          end

          if ~isempty(authmechanism)
              authmechanism = validatestring(p.Results.AuthMechanism,{'MONGODB_CR','PLAIN','SCRAM_SHA_1'});
          end
          
          try
            
              % Invoke UTILITY function to build an ArrayList of SERVER
              % ADDRESSES - Mongo ServerAddress API
              serverAddrList = java.util.ArrayList;
              for i = length(server)
                serverAddrList.add(com.mongodb.ServerAddress(server{i},port(i)));            
              end

                % Set ConnectTimeout and ServerSelectionTimeout options to minimize network traffic 
                options = com.mongodb.MongoClientOptions.builder();
                options = options.connectTimeout(connectiontimeout);
                options = options.serverSelectionTimeout(serverselectiontimeout);
                options = options.sslEnabled(sslEnabled);
                
                if ~isempty(readpreference)                
                    switch readpreference
                        case {'nearest'}
                            options = options.readPreference(com.mongodb.ReadPreference.nearest());

                        case {'primary'}
                            options = options.readPreference(com.mongodb.ReadPreference.primary());

                        case {'secondary'}
                            options = options.readPreference(com.mongodb.ReadPreference.secondary());

                        case {'primarypreferred'}
                            options = options.readPreference(com.mongodb.ReadPreference.primaryPreferred());

                        case {'secondarypreferred'}
                            options = options.readPreference(com.mongodb.ReadPreference.secondaryPreferred());
                    end            
                end

                if ~isempty(writeconcern)                
                    switch writeconcern
                        case {'acknowledged'}
                            options = options.writeConcern(com.mongodb.WriteConcern.ACKNOWLEDGED);

                        case {'majority'}
                            options = options.writeConcern(com.mongodb.WriteConcern.MAJORITY);

                        case {'w1'}
                            options = options.writeConcern(com.mongodb.WriteConcern.W1);

                        case {'w2'}
                            options = options.writeConcern(com.mongodb.WriteConcern.W2);

                        case {'w3'}
                            options = options.writeConcern(com.mongodb.WriteConcern.W3);                        
                    end            
                end

                options = options.build();

                % Create a MONGO DATABASE OBJECT for each DATABASENAME provide

              if ~isempty(username) && ~isempty(password)

                credentialList = java.util.ArrayList;

                % Invoke appropriate JAVA driver method based on
                % AUTHMECHANISM provided
                switch authmechanism

                  case 'MONGODB_CR'
                    credential = com.mongodb.MongoCredential.createMongoCRCredential(username,databasename,password);

                  case 'PLAIN'
                    credential = com.mongodb.MongoCredential.createPlainCredential(username,databasename,password);

                  case 'SCRAM_SHA_1'
                    credential = com.mongodb.MongoCredential.createScramSha1Credential(username,databasename,password);

                  otherwise
                    credential = com.mongodb.MongoCredential.createCredential(username,databasename,password);

                end

                credentialList.add(credential);
                mongodbconn.ConnectionHandle = com.mongodb.MongoClient(serverAddrList,credentialList,options);
                mongodbconn.UserName = username;

              else
            
                mongodbconn.ConnectionHandle = com.mongodb.MongoClient(serverAddrList,options);
                mongodbconn.UserName = '';

              end

              mongodbconn.Database = databasename;

              % Check if authentication passed
              mongodbconn.ConnectionHandle.getAddress;

              % Try to get database instance
              mongodbconn.DatabaseHandle = mongodbconn.ConnectionHandle.getDB(databasename);

              % Construct Mongo object only if connection is successful
              mongodbconn.Server = server;
              
              mongodbconn.Port = port;

              mongodbconn.CollectionNames = {};

              try

                % Invoke Mongo-JAVA Driver API to obtain list of collections
                colllist = mongodbconn.DatabaseHandle.getCollectionNames;

                % Manually iterate through the list and build a CELL ARRAY
                iter = colllist.iterator;

                while (iter.hasNext)
                  mongodbconn.CollectionNames = [mongodbconn.CollectionNames; iter.next()];
                end

                mongodbconn.CollectionNames = mongodbconn.CollectionNames';

              catch

                mongodbconn.CollectionNames = {};

              end

              mongodbconn.TotalDocuments = [];
              try
                mongodbconn.TotalDocuments = mongodbconn.DatabaseHandle.getStats.get('objects');
              catch
                mongodbconn.TotalDocuments = [];
              end

           catch e

              % Clean - up activity
              % First close the connection to the server to avoid
              % orphaned objects
              if ~isempty(mongodbconn.ConnectionHandle)
                mongodbconn.ConnectionHandle.close;              
              end
              error(message('mongodb:mongodb:DriverError',mongo.extractExceptionMessage(e)));

           end

    end
    
  end
  
  methods (Access = public)
    
    % Database-related operations
    
    function close(mongodbconn)
        %CLOSE close mongo database connection
        %
        % Input arguments:
        % ----------------
        % MONGODBCONN - Mongo database object
        %
        % Example:
        % --------
        % close (mongodbconn)
        %
        % Copyright 2017 The MathWorks, Inc.

        validateattributes(mongodbconn,"mongo",{"scalar"});
       
        if ~isopen(mongodbconn)
            if isvalid(mongodbconn)
                mongodbconn.delete;
            end
            return;
        end
        
        % Get connection handle
        connectionHandle = getConnHandle(mongodbconn);
        % Close the connection
        connectionHandle.close;

        % Update the Object properties
        mongodbconn.delete;

    end        
    
    function val = isopen(mongodbconn)
        %ISOPEN Check if database connection is valid
        %
        % returns true(1) if database connection is open ,else returns
        % false(0)
        %
        % Input arguments:
        % ----------------
        % mongodbconn - Mongo database object
        %
        % Example:
        % --------
        % val = isopen (mongodbconn)
        % 
        % Copyright 2017 The MathWorks, Inc.

        % MONGODBCONN should be SCALAR
        validateattributes(mongodbconn,"mongo",{"scalar"});

        if ~isvalid(mongodbconn)
            val = false;
            return;
        end

        if isempty(getConnHandle(mongodbconn))
            val = false;
            return;
        end
        
        val = true;
    end
    
    function val = count(mongodbconn,collectname,varargin)
        
        %COUNT returns the total documents in a collection or a Mongo query
        % 
        % VAL = COUNT(MONGODBCONN, COLLECTNAME)
        % returns the count of all documents in a given COLLECTNAME
        %
        % VAL = COUNT(MONGODBCONN,COLLECTNAME,'NAME','VALUE')
        % returns the count of documents with additional name-value
        % arguments. For instance, you can check total documents for a
        % Mongo Query against a collection.
        %
        % Input Arguments:
        % ---------------
        % MONGODBCONN - Mongo database object. 
        % COLLECTNAME - Collection name.
        %
        % Name-Value arguments:                                       
        % ---------------------
        % Query       - JSON-Style Mongo Query.
        %
        % Example:
        % --------
        %
        % val = count(mongodbconn,"product")
        %
        % val = count(mongodbconn,"product","Query",'{"artist":"David"}')
        %
        % Copyright 2017 The MathWorks, Inc.

        % IMPORT JSON parser
            
        if (exist('com.mongodb.util.JSON','class') ~= 8)
              error(message('mongodb:mongodb:driverNotFound'));
        end
          
        p = inputParser;

        p.addRequired("mongodbconn",@(x)validateattributes(x,"mongo",{"scalar"}));
        p.addRequired("collectname",@(x)validateattributes(x,["string","char"],{"scalartext"}));
        p.addParameter("Query","",@(x)validateattributes(x,["string","char"],{"scalartext"}));
        
        p.parse(mongodbconn,collectname,varargin{:});
        
        query = char(p.Results.Query);
        collectname = char(p.Results.collectname);
        
        % Check if Mongo Database Object is valid
        if ~isopen(mongodbconn)
          error(message('mongodb:mongodb:InvalidMongoConnection'));
        end

        % Get the HANDLE to DATABASE object
        databaseHandle = getDBHandle(mongodbconn);

        % First check if a collection by the name COLLECTNAME exists in
        % the DATABASE using Mongo-JAVA driver API COLLECTIONEXISTS
        coll_exists = collectionexists(mongodbconn, collectname);

        % If COLLECTION exists create a Mongo Collection Object
        % else, return empty object with error message
        if coll_exists
            try
                collectionHandle = databaseHandle.getCollection(collectname);
            catch e
                error(message('mongodb:mongodb:DriverError',mongo.extractExceptionMessage(e)))
            end
        else
            error(message('mongodb:mongodb:NonExistentMongoCollection',collectname));
        end

        try
            val = collectionHandle.count(com.mongodb.util.JSON.parse(query));
          catch e
            error(message('mongodb:mongodb:DriverError',mongo.extractExceptionMessage(e)))
        end

    end
    
    function val = distinct(mongodbconn,collectname,fieldname,varargin)
        
        %DISTINCT find distinct values for a specified field across a
        %collection
        %
        % VAL = DISTINCT(MONGODBCONN,COLLECTNAME,FIELDNAME)
        % returns distinct values in a field of a collection
        %
        % VAL = DISTINCT(MONGODBCONN,COLLECTNAME,FIELDNAME,'NAME','VALUE')
        % returns distinct values in a field with additional name-value arguments.
        % For instance, you can find distinct values in a field for a Mongo
        % query executed against a collection
        %
        % Input arguments:
        % ----------------
        % MONGODBCONN - Mongo database object
        % COLLECTNAME - Collection name
        % FIELDNAME   - Field name stored in a document of the collection
        %
        % Name-Value arguments:
        % ---------------------
        % Query       - JSON-style Mongo Query.
        %
        % EXAMPLE:
        % --------
        %
        % val = distinct(mongodbconn,"product","songs")
        %
        % val = distinct(mongodbconn,"product","songs","Query",'{"artist":"David"}')
        %
        % Copyright 2017 The MathWorks, Inc.

        % IMPORT JSON parser

        if (exist('com.mongodb.util.JSON','class') ~= 8 || ...
            exist('com.mongodb.client.model.DBCollectionDistinctOptions','class') ~= 8 || ...
            exist('com.mongodb.ReadPreference','class') ~= 8)            
              error(message('mongodb:mongodb:driverNotFound'));
        end

        p = inputParser;

        p.addRequired("mongodbconn",@(x)validateattributes(x,"mongo",{"scalar"}));
        p.addRequired("collectname",@(x)validateattributes(x,["string","char"],{"scalartext"}));
        p.addRequired("fieldname",@(x)validateattributes(x,["string","char"],{"scalartext"}));
        p.addParameter("Query","",@(x)validateattributes(x,["string","char"],{"scalartext"}));
        p.addParameter("ReadPreference","",@(x)validateattributes(x,["string" "char"],{"scalartext"}));
        
        p.parse(mongodbconn,collectname,fieldname,varargin{:});
        
        collectname = char(p.Results.collectname);
        fieldname = char(p.Results.fieldname);
        query = char(p.Results.Query);
        readpreference = char(p.Results.ReadPreference);
        
        if ~isempty(readpreference)
            readpreference = validatestring(readpreference,{'nearest','primary','secondary','primarypreferred','secondarypreferred'});
        end
        
        % Check if Mongo Database Object is valid
        if ~isopen(mongodbconn)
          error(message('mongodb:mongodb:InvalidMongoConnection'));
        end

        % Get the HANDLE to DATABASE object
        databaseHandle = getDBHandle(mongodbconn);

        % First check if a collection by the name COLLECTNAME exists in
        % the DATABASE using Mongo-JAVA driver API COLLECTIONEXISTS
        coll_exists = collectionexists(mongodbconn, collectname);

        % If COLLECTION exists create a Mongo Collection Object
        % else, return empty object with error message
        if coll_exists
            try
                collectionHandle = databaseHandle.getCollection(collectname);
            catch e
                error(message('mongodb:mongodb:DriverError',mongo.extractExceptionMessage(e)))
            end
        else
            error(message('mongodb:mongodb:NonExistentMongoCollection',collectname));
        end

        try
            distinctoptions = com.mongodb.client.model.DBCollectionDistinctOptions;
            
            if ~isempty(query)
                distinctoptions = distinctoptions.filter(com.mongodb.util.JSON.parse(query));
            end
            
            if ~isempty(readpreference)
                switch readpreference
                    case {'nearest'}
                        distinctoptions = distinctoptions.readPreference(com.mongodb.ReadPreference.nearest());
                        
                    case {'primary'}
                        distinctoptions = distinctoptions.readPreference(com.mongodb.ReadPreference.primary());
                        
                    case {'secondary'}
                        distinctoptions = distinctoptions.readPreference(com.mongodb.ReadPreference.secondary());
                        
                    case {'primarypreferred'}
                        distinctoptions = distinctoptions.readPreference(com.mongodb.ReadPreference.primaryPreferred());
                        
                    case {'secondarypreferred'}
                        distinctoptions = distinctoptions.readPreference(com.mongodb.ReadPreference.secondaryPreferred());
                end            
            end
            
            data = collectionHandle.distinct(fieldname,distinctoptions);
            val = cell(data.toArray)';
          catch e
            error(message('mongodb:mongodb:DriverError',mongo.extractExceptionMessage(e)))
        end

    end

    function documents = find(mongodbconn,collectname,varargin)
        %FIND executes a MONGO QUERY on the database and retrieve documents
        %
        % DOCUMENTS = FIND(MONGODBCONN,COLLECTNAME)
        % returns all documents in a collection
        %
        % DOCUMENTS = FIND(MONGODBCONN,COLLECTNAME,'NAME','VALUE')
        % returns documents based on Name-Value arguments. For instance,
        % you can specify a Mongo Query to filter documents.
        %
        % Input Arguments:
        % ----------------
        %
        % MONGODBCONN - Mongo database object
        % COLLECTNAME - Collection name
        %
        % Name-Value arguments:
        % ---------------------
        %
        %   Query          A JSON-style MONGO Query used to 
        %                  filter documents in a collection and return 
        %                  only those who meets the criteria in Query
        %                  E.g., {"artist": "David"}
        %
        %   Projection     A JSON-style MONGO Projection key used to 
        %                  specify which KEY(s) should be projected 
        %                  in documents which are returned 
        %                  E.g., {"song":1.0}
        %
        %   Sort           A JSON-style MONGO Sort key used to 
        %                  specify which KEY(s) should be used to 
        %                  sort documents which are returned
        %                  E.g., {"_id": 1.0}
        %
        %   Skip           A double value which skips specified number
        %                  of documents from start in the resultset before
        %                  it is received in MATLAB
        %                  E.g., if a Query returns 10000 documents, SKIP of 1000
        %                  means,only last 9000 documents will be retrieved
        %
        %   Limit          A double value which limits number of
        %                  documents to be retrieved
        %                  E.g., if a Query returns 10000 documents, LIMIT of 1000
        %                  means,first 1000 documents will be retrieved
        %
        % Example:
        % --------
        %
        % documents = find(mongodbconn,"product")
        %
        % documents = find(mongodbconn,"product","Query",'{"artist":"David"}')
        %
        % documents = find(mongodbconn,"product","Projection",'{"song":1.0}')
        %
        % documents = find(mongodbconn,"product","Query",'{"artist":"David"}',"Projection",'{"song":1.0}') 
        %
        % documents = find(mongodbconn,"product","Query",'{"artist":"David"}',"Sort",'{"_id":1.0}')
        %
        % documents = find(mongodbconn,"product","Query",'{"artist":"David"}',"Limit",10)
        %
        % documents = find(mongodbconn,"product","Query",'{"artist":"David"}',"Skip",10)
        %
        % Copyright 2017 The MathWorks, Inc.
            
        if (exist('com.mongodb.util.JSON','class') ~= 8 || ...
            exist('com.mongodb.client.model.DBCollectionFindOptions','class') ~= 8 || ...
            exist('com.mongodb.ReadPreference','class') ~= 8)            
              error(message('mongodb:mongodb:driverNotFound'));
        end

        p = inputParser;

        p.addRequired("mongodbconn",@(x)validateattributes(x,"mongo",{"scalar"}));
        p.addRequired("collectname",@(x)validateattributes(x,["string","char"],{"scalartext"}));
        p.addParameter("Query","",@(x)validateattributes(x,["string","char"],{"scalartext"}));
        p.addParameter("Projection","",@(x)validateattributes(x,["string","char"],{"scalartext"}));
        p.addParameter("Sort","",@(x)validateattributes(x,["string","char"],{"scalartext"}));
        p.addParameter("Skip",0,@(x)validateattributes(x,"numeric",{"scalar","nonnan","nonnegative","finite"}));
        p.addParameter("Limit",0,@(x)validateattributes(x,"numeric",{"scalar","nonnan","nonnegative","finite"}));
        p.addParameter("ReadPreference","",@(x)validateattributes(x,["string" "char"],{"scalartext"}));
        
        p.parse(mongodbconn, collectname, varargin{:});
        
        % Check if Mongo Database Object is valid
        if ~isopen(mongodbconn)
          error(message('mongodb:mongodb:InvalidMongoConnection'));
        end

        databaseHandle = getDBHandle(mongodbconn);

        % First check if a collection by the name COLLECTNAME exists in
        % the DATABASE using Mongo-JAVA driver API COLLECTIONEXISTS
        coll_exists = collectionexists(mongodbconn, collectname);

        % If COLLECTION exists create a Mongo Collection Object
        % else, return empty object with error message
        if coll_exists
            try
                collectionHandle = databaseHandle.getCollection(collectname);
            catch e
                error(message('mongodb:mongodb:DriverError',mongo.extractExceptionMessage(e)));
            end

        else
            error(message('mongodb:mongodb:NonExistentMongoCollection',collectname));
        end

        query = char(p.Results.Query);
        projection = char(p.Results.Projection);
        sort = char(p.Results.Sort);
        skip = p.Results.Skip;
        limit = p.Results.Limit;
        
        readpreference = char(p.Results.ReadPreference);
        
        if ~isempty(readpreference)
            readpreference = validatestring(readpreference,{'nearest','primary','secondary','primarypreferred','secondarypreferred'});
        end

        try
            findoptions = com.mongodb.client.model.DBCollectionFindOptions;
            
            if limit
                findoptions = findoptions.limit(limit);
            end
            
            if skip 
                findoptions = findoptions.skip(skip);
            end
            
            if ~isempty(projection)
                findoptions = findoptions.projection(com.mongodb.util.JSON.parse(projection));
            end
            
            if ~isempty(sort)
                findoptions = findoptions.sort(com.mongodb.util.JSON.parse(sort));
            end
            
            if ~isempty(readpreference)
                switch readpreference
                    case {'nearest'}
                        findoptions = findoptions.readPreference(com.mongodb.ReadPreference.nearest());
                        
                    case {'primary'}
                        findoptions = findoptions.readPreference(com.mongodb.ReadPreference.primary());
                        
                    case {'secondary'}
                        findoptions = findoptions.readPreference(com.mongodb.ReadPreference.secondary());
                        
                    case {'primarypreferred'}
                        findoptions = findoptions.readPreference(com.mongodb.ReadPreference.primaryPreferred());
                        
                    case {'secondarypreferred'}
                        findoptions = findoptions.readPreference(com.mongodb.ReadPreference.secondaryPreferred());
                end            
            end
                        
            cursorHandle = collectionHandle.find(com.mongodb.util.JSON.parse(query),findoptions);
            
        catch e
            error(message('mongodb:mongodb:DriverError',mongo.extractExceptionMessage(e)));
        end

        documents = fetchData(cursorHandle);

        %clear collectionHandle;

        function documents = fetchData(cursorHandle)

          resultset_size = cursorHandle.size();
          documents = [];
          
          a = cursorHandle.one;
          
          if isempty(a)
              return;
          end
          
          a = jsondecode(char(a.toJson));
          s = whos('a');

          r = java.lang.Runtime.getRuntime;
          freeMem = r.freeMemory;
          totalMem = r.totalMemory;
          availMem = totalMem - freeMem;

          try
              needMem = s.bytes * resultset_size;

              if needMem > availMem
                new_resultset_size = floor(availMem/s.bytes);
                cursorHandle.limit(new_resultset_size);
                warning(message('mongodb:mongodb:InsufficientMemoryWarning',num2str(new_resultset_size)));
              end

              a = cursorHandle.toArray().toString();
              %documents = matlab.internal.webservices.fromJSON(char(a));
              documents = jsondecode(char(a));
              cursorHandle.close;
              
          catch ex

            cursorHandle.close            
            error(message('mongodb:mongodb:DriverError',mongo.extractExceptionMessage(ex)));

          end
    
        end
    
    end
    
    function createCollection(mongodbconn,collectname,varargin)
            %CREATECOLLECTION creates a new collection in database
            %
            % CREATECOLLECTION(MONGODBCONN,COLLECTNAME)
            % creates a new collection in database.
            %
            % Input Arguments:
            % ----------------
            %
            % MONGODBCONN - Mongo database object
            % COLLECTNAME - Collection name
            %           
            % Example:
            % --------
            %
            % createCollection(dbconn,"product")
            %
            % Copyright 2017 The MathWorks, Inc.
            
            if (exist('com.mongodb.BasicDBObject','class') ~= 8)            
                error(message('mongodb:mongodb:driverNotFound'));
            end
            
            p = inputParser;

            p.addRequired("mongodbconn",@(x)validateattributes(x,"mongo",{"scalar"}));
            p.addRequired("collectname",@(x)validateattributes(x,["string","char"],{"scalartext"}));
            p.addParameter("Capped",false,@(x)validateattributes(x,"logical",{"scalar"}));
            p.addParameter("Size",0,@(x)validateattributes(x,"numeric",{"nonempty","scalar","nonnegative"}));
            p.addParameter("Max",0,@(x)validateattributes(x,"numeric",{"nonempty","scalar","nonnegative"}));

            p.parse(mongodbconn,collectname,varargin{:});
            
            % Check if Mongo Database Object is valid
            if ~isopen(mongodbconn)
                error(message('mongodb:mongodb:InvalidMongoConnection'));
            end
            
            collectname = char(p.Results.collectname);
            capped = p.Results.Capped;
            size = p.Results.Size;
            max = p.Results.Max;
            
            coll_exists = collectionexists(mongodbconn, collectname);
            
            % If COLLECTION exists then no point in creating new collection
            % with same name
            if coll_exists
                error(message('mongodb:mongodb:MongoCollectionExists',collectname));
            end
            
            try
                databaseHandle = getDBHandle(mongodbconn);
                options = com.mongodb.BasicDBObject();
            
                options.put("capped",capped);

                if size
                    options.put("size",size);
                end

                if max && size
                    options.put("max",max);
                end
            
                databaseHandle.createCollection(collectname,options);
                
            catch e
                error(message('mongodb:mongodb:DriverError',mongo.extractExceptionMessage(e)));
            end
    end
        
    function dropCollection(mongodbconn,collectname)
            %DROPCOLLECTION removes entire collection from database
            %
            % DROPCOLLECTION(MONGODBCONN,COLLECTNAME)
            % drops entire collection from database
            %
            % Input Arguments:
            % ----------------
            % MONGODBCONN - Mongo database object
            % COLLECTNAME - Collection name
            %
            % Example:
            % --------
            %
            % dropCollection(dbconn,"product")
            %
            % Copyright 2017 The MathWorks, Inc.
            
            p = inputParser;

            p.addRequired("mongodbconn",@(x)validateattributes(x,"mongo",{"scalar"}));
            p.addRequired("collectname",@(x)validateattributes(x,["string","char"],{"scalartext"}));
            
            p.parse(mongodbconn,collectname);
            
            % Check if Mongo Database Object is valid
            if ~isopen(mongodbconn)
                error(message('mongodb:mongodb:InvalidMongoConnection'));
            end
            
            collectname = char(p.Results.collectname);
            coll_exists = collectionexists(mongodbconn, collectname);
            
            % If COLLECTION exists then no point in creating new collection
            % with same name
            if ~coll_exists
                error(message('mongodb:mongodb:NonExistentMongoCollection',collectname));
            end
            
            databaseHandle = getDBHandle(mongodbconn);
            databaseHandle.getCollection(collectname).drop();
            
    end
  
    function deletecount = remove(mongodbconn,collectname,query,varargin)
            %REMOVE remove documents in a collection
            %
            % DELETEDCOUNT = REMOVE(MONGODBCONN,COLLECTNAME,QUERY)
            % remove documents in a collection which matches specified
            % condition.
            %
            % Input arguments:
            % ----------------
            %
            % MONGODBCONN - Mongo database object
            % COLLECTNAME - Collection name
            %
            % Name-Value argument:
            % --------------------
            % Query       - JSON-style Mongo Query.
            %
            %
            % Example:
            % --------
            % deletecount = remove(mongodbconn,"product",'{}')
            %   removes all documents in 'product' collection
            %
            % deletecount = remove(mongodbconn,"product",'{"artist":"davis"}')
            %   removes all documents for 'davis' in 'product' collection
            %
            % Copyright 2017 The MathWorks, Inc.
            
            if (exist('com.mongodb.util.JSON','class') ~= 8 || ...            
                exist('com.mongodb.client.model.DBCollectionRemoveOptions','class') ~= 8 || ...
                exist('com.mongodb.WriteConcern','class') ~= 8)
                error(message('mongodb:mongodb:driverNotFound'));
            end
            
            p = inputParser;

            p.addRequired("mongodbconn",@(x)validateattributes(x,"mongo",{"scalar"}));
            p.addRequired("collectname",@(x)validateattributes(x,["string","char"],{"scalartext"}));
            p.addRequired("query",@(x)validateattributes(x,["string","char"],{"scalartext"}));
            p.addParameter("WriteConcern","",@(x)validateattributes(x,["string" "char"],{"scalartext"}));
            
            p.parse(mongodbconn,collectname,query,varargin{:});
            
            % Check if Mongo Database Object is valid
            if ~isopen(mongodbconn)
                error(message('mongodb:mongodb:InvalidMongoConnection'));
            end
        
            collectname = char(p.Results.collectname);
            findquery = char(p.Results.query);
            writeconcern = char(p.Results.WriteConcern);
            
            if ~isempty(writeconcern)
                writeconcern = validatestring(writeconcern,{'acknowledged','majority','w1','w2','w3'});
            end
            
            databaseHandle = getDBHandle(mongodbconn);
            coll_exists = collectionexists(mongodbconn, collectname);
            
            % Ensure collection exists
            if ~coll_exists
                error(message('mongodb:mongodb:NonExistentMongoCollection',collectname));
            end
                
            try               
                removeoptions = com.mongodb.client.model.DBCollectionRemoveOptions;
                
                if ~isempty(writeconcern)
                    switch writeconcern
                        case {'acknowledged'}
                            removeoptions = removeoptions.writeConcern(com.mongodb.WriteConcern.ACKNOWLEDGED);

                        case {'majority'}
                            removeoptions = removeoptions.writeConcern(com.mongodb.WriteConcern.MAJORITY);

                        case {'w1'}
                            removeoptions = removeoptions.writeConcern(com.mongodb.WriteConcern.W1);

                        case {'w2'}
                            removeoptions = removeoptions.writeConcern(com.mongodb.WriteConcern.W2);

                        case {'w3'}
                            removeoptions = removeoptions.writeConcern(com.mongodb.WriteConcern.W3);
                    end            
                end
                
                collectionHandle = databaseHandle.getCollection(collectname);
                deleteResult = collectionHandle.remove(com.mongodb.util.JSON.parse(findquery),removeoptions);
                deletecount = deleteResult.getN();                
            catch e 
                error(message('mongodb:mongodb:DriverError',mongo.extractExceptionMessage(e)));
            end
    end  
    
    function updateCount = update(mongodbconn,collectname,findquery,updatequery,varargin)
            %UPDATE update documents in a collection
            %
            % UPDATECOUNT = update(MONGODBCONN,COLLECTNAME,FINDQUERY,UPDATEQUERY)
            % update documents in a collection
            %
            % Input arguments:
            % ----------------
            %
            % MONGODBCONN - Mongo database object
            % COLLECTNAME - Collection name
            % FINDQUERY   - JSON-style Mongo query to find documents to
            %               update
            % UPDATEQUERY - JSON-style Mongo query to update found
            %               documents
            %
            % Example:
            % --------
            % updatecount = remove(mongodbconn,"product",'{"artist":"davis"}','{$set:{"release":1990}}')
            %
            % Copyright 2017 The MathWorks, Inc.
            
            if (exist('com.mongodb.util.JSON','class') ~= 8 || ...            
                exist('com.mongodb.client.model.DBCollectionUpdateOptions','class') ~= 8 || ...
                exist('com.mongodb.WriteConcern','class') ~= 8)
                error(message('mongodb:mongodb:driverNotFound'));
            end
            
            import java.lang.Boolean;
            
            p = inputParser;

            p.addRequired("mongodbconn",@(x)validateattributes(x,"mongo",{"scalar"}));
            p.addRequired("collectname",@(x)validateattributes(x,["string","char"],{"scalartext"}));
            p.addRequired("findquery",@(x)validateattributes(x,["string","char"],{"scalartext"}));
            p.addRequired("updatequery",@(x)validateattributes(x,["string","char"],{"scalartext"}));
            p.addParameter("ByPassValidation",false,@(x)validateattributes(x,"logical",{"scalar"}));
            p.addParameter("Upsert",false,@(x)validateattributes(x,"logical",{"scalar"}));
            p.addParameter("UpdateMulti",true,@(x)validateattributes(x,"logical",{"scalar"}));
            p.addParameter("WriteConcern","",@(x)validateattributes(x,["string" "char"],{"scalartext"}));
            
            p.parse(mongodbconn,collectname,findquery,updatequery,varargin{:});
            
            % Check if Mongo Database Object is valid
            if ~isopen(mongodbconn)
              error(message('mongodb:mongodb:InvalidMongoConnection'));
            end
            
            collectname = char(p.Results.collectname);
            findquery = char(p.Results.findquery);
            updatequery = char(p.Results.updatequery);
            
            bypassvalidation = p.Results.ByPassValidation;
            upsert = p.Results.Upsert;
            updatemulti = p.Results.UpdateMulti;
            
            writeconcern = char(p.Results.WriteConcern);
            
            if ~isempty(writeconcern)
                writeconcern = validatestring(writeconcern,{'acknowledged','majority','w1','w2','w3'});
            end
            
            databaseHandle = getDBHandle(mongodbconn);
            coll_exists = collectionexists(mongodbconn, collectname);
            
            % Ensure collection exists
            if ~coll_exists
                error(message('mongodb:mongodb:NonExistentMongoCollection',collectname));
            end
                
            try               
                updateoptions = com.mongodb.client.model.DBCollectionUpdateOptions;
                
                if bypassvalidation
                    updateoptions = updateoptions.bypassDocumentValidation(Boolean.TRUE);
                else
                    updateoptions = updateoptions.bypassDocumentValidation(Boolean.FALSE);
                end
                updateoptions = updateoptions.multi(updatemulti);
                updateoptions = updateoptions.upsert(upsert);
                
                if ~isempty(writeconcern)
                    switch writeconcern
                        case {'acknowledged'}
                            updateoptions = updateoptions.writeConcern(com.mongodb.WriteConcern.ACKNOWLEDGED);

                        case {'majority'}
                            updateoptions = updateoptions.writeConcern(com.mongodb.WriteConcern.MAJORITY);

                        case {'w1'}
                            updateoptions = updateoptions.writeConcern(com.mongodb.WriteConcern.W1);

                        case {'w2'}
                            updateoptions = updateoptions.writeConcern(com.mongodb.WriteConcern.W2);

                        case {'w3'}
                            updateoptions = updateoptions.writeConcern(com.mongodb.WriteConcern.W3);
                    end            
                end
                
                collectionHandle = databaseHandle.getCollection(collectname);
                deleteResult = collectionHandle.update(com.mongodb.util.JSON.parse(findquery),com.mongodb.util.JSON.parse(updatequery),updateoptions);
                updateCount = deleteResult.getN();                
            catch e 
                error(message('mongodb:mongodb:DriverError',mongo.extractExceptionMessage(e)));
            end
    end  
    
    function insertCount = insert(mongodbconn,collectname,documents,varargin)
            %INSERT insert documents in a collection
            %
            % INSERTCOUNT = INSERT(MONGODBCONN,COLLECTNAME,DOCUMENTS)
            % INSERT dcouments in a collection
            %
            % Input arguments:
            % ----------------
            %
            % MONGODBCONN - Mongo database object
            % COLLECTNAME - Collection name
            % DOCUMENTS   - MATLAB struct, struct array, table,
            %               containers.Map object, character vector, string represeting
            %               data to be inserted to MongoDB.
            %
            % Example:
            % --------
            % insertcount = insert(mongodbconn,"product",'{"key1":"value1","key2":"value2"}')
            %
            % Copyright 2017 The MathWorks, Inc.
            
            if (exist('com.mongodb.util.JSON','class') ~= 8 || ...            
                exist('com.mongodb.InsertOptions','class') ~= 8 || ...
                exist('com.mongodb.WriteConcern','class') ~= 8)
                error(message('mongodb:mongodb:driverNotFound'));
            end
            
            import java.util.ArrayList;
            import java.lang.Boolean;
            
            p = inputParser;
        
            p.addRequired("mongodbconn",@(x)validateattributes(x,"mongo",{"scalar"}));
            p.addRequired("collectname",@(x)validateattributes(x,["string","char"],{"scalartext"}));
            p.addRequired("documents",@(x)validateattributes(x,["string","char","cell","struct","table","containers.Map", "" + class(documents) + ""],{"nonempty"}));
            p.addParameter("ByPassValidation",false,@(x)validateattributes(x,"logical",{"scalar"}));
            p.addParameter("ContinueOnError",false,@(x)validateattributes(x,"logical",{"scalar"}));
            p.addParameter("WriteConcern","",@(x)validateattributes(x,["string" "char"],{"scalartext"}));
          
            p.parse(mongodbconn,collectname,documents,varargin{:});
            
            
            % Check if Mongo Database Object is valid
            if ~isopen(mongodbconn)
              error(message('mongodb:mongodb:InvalidMongoConnection'));
            end
            
            collectname = char(p.Results.collectname);
            documents = p.Results.documents;
            writeconcern = char(p.Results.WriteConcern);
            
            if ~isempty(writeconcern)
                writeconcern = validatestring(writeconcern,{'acknowledged','majority','w1','w2','w3'});
            end
            
            
            switch class(documents)
                
                case {'table'}
                    
                    documents = jsonencode(documents);
                    
                case {'string'}
                    
                    if documents.strlength == 0
                        error(message('mongodb:mongodb:ExpectedNonempty','''documents'''));
                    end
                    
                    validateattributes(documents,{'string'},{'scalartext'});
                    documents = char(documents);
                    
                case {'char'}
                    
                    validateattributes(documents,{'char'},{'scalartext'});
                    documents = char(documents);
                    
                case {'cell'}
                    
                    if ~all(cellfun(@isstruct,documents))
                        error(message('mongodb:mongodb:InsertError','cell array of structures'));
                    end
                    
                case {'struct'}
                    
                    % do nothing
                    
                case {'containers.Map'}
                    
                    documents = jsonencode(documents);
                    
                otherwise
                    
                    documents = jsonencode(documents);
            end
                
            bypassvalidation = p.Results.ByPassValidation;
            continueonerror = p.Results.ContinueOnError;
            
            databaseHandle = getDBHandle(mongodbconn);
            coll_exists = collectionexists(mongodbconn, collectname);
            
            % Ensure collection exists
            if ~coll_exists
                error(message('mongodb:mongodb:NonExistentMongoCollection',collectname));
            end
                
            try               
                insertoptions = com.mongodb.InsertOptions;
                if bypassvalidation
                    insertoptions = insertoptions.bypassDocumentValidation(Boolean.TRUE);
                else
                    insertoptions = insertoptions.bypassDocumentValidation(Boolean.FALSE);
                end
                insertoptions = insertoptions.continueOnError(continueonerror);
                
                if ~isempty(writeconcern)
                    switch writeconcern
                        case {'acknowledged'}
                            insertoptions = insertoptions.writeConcern(com.mongodb.WriteConcern.ACKNOWLEDGED);

                        case {'majority'}
                            insertoptions = insertoptions.writeConcern(com.mongodb.WriteConcern.MAJORITY);

                        case {'w1'}
                            insertoptions = insertoptions.writeConcern(com.mongodb.WriteConcern.W1);

                        case {'w2'}
                            insertoptions = insertoptions.writeConcern(com.mongodb.WriteConcern.W2);

                        case {'w3'}
                            insertoptions = insertoptions.writeConcern(com.mongodb.WriteConcern.W3);
                    end            
                end
            
                collectionHandle = databaseHandle.getCollection(collectname);
                
                myList = java.util.ArrayList;
                
                if isstruct(documents)
                    for i = 1:length(documents)
                        myList.add(com.mongodb.util.JSON.parse(jsonencode(documents(i))));
                    end
                end
                
                if iscell(documents)
                   for i = 1:length(documents)
                        myList.add(com.mongodb.util.JSON.parse(jsonencode(documents{i})));
                    end
                end 
                
                if ischar(documents)
                    mongodbtype = com.mongodb.util.JSON.parse(documents);
                    
                    if isa(mongodbtype,'com.mongodb.BasicDBObject')
                        myList.add(mongodbtype);
                    end
                    
                    if isa(mongodbtype,'com.mongodb.BasicDBList')
                        it = mongodbtype.iterator;
                        while(it.hasNext)
                            myList.add(it.next);
                        end
                    end
                end
                
                countbeforeinsert = count(mongodbconn,collectname);
                collectionHandle.insert(myList,insertoptions);
                countafterinsert = count(mongodbconn,collectname);
                insertCount = countafterinsert - countbeforeinsert;                
            catch e 
                error(message('mongodb:mongodb:DriverError',mongo.extractExceptionMessage(e)));
            end
    end 
    
  end

  methods(Hidden = true)            
      
      % closes all MONGO Connection objects before going out of context
      function delete(obj)   
        if isvalid(obj)
          close(obj);
        end
      end
      
          % Collection-related operations
    
    function val = collectionexists(mongodbconn,collectname)
        
        %COLLECTIONEXISTS checks if a collection exists in the database.
        %
        % VAL = COLLECTIONEXISTS(MONGODBCONN,COLLECTNAME)
        % returns true(1) if collection exists in the database, otherwise
        % returns false (0).
        %
        % Input arguments:
        % ----------------
        % MNGODBCONN - Mongo database object
        % COLLECTNAME - Collection name
        %
        % Example:
        % --------
        % val = collectionexists(mongodbconn,"product")
        %  
        % Copyright 2017 The MathWorks, Inc.

        p = inputParser;

        p.addRequired("mongodbconn",@(x)validateattributes(x,"mongo",{"scalar"}));
        p.addRequired("collectname",@(x)validateattributes(x,["string","char"],{"scalartext"}));
        
        p.parse(mongodbconn,collectname);
        
        % Check if Mongo Database Object is valid
        if ~isopen(mongodbconn)
          error(message('mongodb:mongodb:InvalidMongoConnection'));
        end

        databaseHandle = getDBHandle(mongodbconn);

        collectname = char(p.Results.collectname);

        % Invoke Mongo-JAVA Driver API to check if collection exists on the database
        try

            val = databaseHandle.collectionExists(collectname);

        catch e
            throw(e);            
        end

    end
    
  end
  
  % protected methods
  methods (Access = 'protected')
    % this allows for non-standard display for connection object.
    function displayScalarObject(conn)
        %displayScalarObject controls the display of the MONGO object.

        %   Copyright 2017 The MathWorks, Inc.

        import database.internal.utilities.cellArrayDisp;
        import database.internal.utilities.charArrayDisp;

        % header
        header = matlab.mixin.CustomDisplay.getSimpleHeader(conn);
        disp(header);

        % Default Properties

        disp(['    ', '           Database: ''', conn.Database, '''']);
        disp(['    ', '           UserName: ''', conn.UserName, '''']);

        serverDisp = cellArrayDisp(conn.Server,'');
        disp(['    ', '             Server: ', serverDisp]);
        disp(['    ', '               Port: ', num2str(conn.Port)]);

        collectionNamesDisp = cellArrayDisp(conn.CollectionNames,'');
        disp(['    ', '    CollectionNames: ', collectionNamesDisp]);
        disp(['    ', '     TotalDocuments: ', num2str(conn.TotalDocuments)]);
        fprintf('\n');

    end
  end
    

end