# MLAPI.ServerList
The MLAPI.ServerList is a server list designed for video games. It's built of one server application that can shard horizontally and a client library that can register servers and make queries. It's built to be high performance, feature rich and general purpose.

## Features
* Cross platform
* Search for servers with the MongoDB query language (sub and super set of the official language)
* Shards horizontally with MongoDB
* Can run without sharding in memory
* Custom data is contracted
* Custom data contracts can be changed and be backwards compatible

## TODO
* File database to reduce memory usage when not using shards
* TLS

## Usage
The ServerList has two parts, a server and a client library.

### Server
The server is a .NET Core application. When started, it generates a configuration file called config.json. The default configuration looks like this:

```json
{
  "VerbosePrints": true,
  "Port": 9423,
  "ListenAddress": "0.0.0.0",
  "UseMongo": false,
  "MongoConnection": "mongodb://127.0.0.1:27017",
  "CollectionExpiryDelay": 1200000,
  "MongoDatabase": "listserver",
  "ServerTimeout": 20000,
  "ServerContract": [
    {
      "Name": "Name",
      "Type": "String",
      "Required": false
    },
    {
      "Name": "Players",
      "Type": "Int32",
      "Required": true
    }
  ]
}
```

The options are:

##### VerbosePrints
Whether or not prints should include information that **might** be sensitive such as the unique Id of a server.

##### Port
The TCP port the server should run on.

##### ListenAddress
The local address to listen on. Defaults to 0.0.0.0 which means all interfaces.

##### UseMongo
Whether or not to use MongoDB as the backend. This allows you to shard horizontally.

##### MongoConnection
The MongoDB connection string. Only required if UseMongo is true.

##### MongoDatabase
The database to use in Mongo to store the servers. Only required if UseMongo is true.

##### CollectionExpiryDelay
The amount of time a server is in memory before being cleared out.

##### ServerTimeout
The amount of time a server has to send a heartbeat before it is concidered dead.

##### ServerContract
This is the contract that defines what fields are possible, required for a server to define and also for a client to query on. See the contracts section below.

### Client
The client library has two parts, one part is the part which a game server runs, and the other one is what a game client would run. An example can be seen below:

```csharp
// Creates a connection object
ServerConnection advertConnection = new ServerConnection();

// Connect to the ListServer
advertConnection.Connect("127.0.0.1", 9423);

// Create some data about our server
Dictionary<string, object> data = new Dictionary<string, object>
{
    { "Players", 25 },
    { "Name", "This is the name" }
};

// Announce our server to the ListServer
advertConnection.StartAdvertisment(data);
```

Additionaly, the server can stop advertising or update its advertisment data. This is useful for having a "Players" count that is changed every time a player connects or leaves.

```csharp
// Create some new data about our server
Dictionary<string, object> data = new Dictionary<string, object>
{
    { "Players", 30 },
    { "Name", "This is the name" }
};

// Updates the data on the server
advertConnection.UpdateAdvertismentData(data);
```

```csharp
// Stops advertising to the server
advertConnection.StopAdvertising();
```

For game clients, they can query the ListServer like this:
```csharp
// Disposing a ServerConnection will close the connection and clean itself up.
using (ServerConnection queryConnection = new ServerConnection())
{
    // Connect
    queryConnection.Connect("127.0.0.1", 9423);

    // Send query to find all servers with players >= 20 AND <= 50. This requires a Players field to be present.
    List<ServerModel> models = queryConnection.SendQuery(@"
    {
        ""$and"": [
            {
                ""Players"": {
                    ""$gte"": 20
                }
            },
            {
                ""Players"": {
                    ""$lte"": 50
                }
            }
        ]
    }");

    // Prints header
    Console.WriteLine(string.Format("| {0,5} | {1,5} | {2,5} |", "UUID", "Name", "Players"));
    // Prints the servers
    Console.WriteLine(string.Join(Environment.NewLine, models.Select(x => string.Format("| {0,5} | {1,5} | {2,5} |", x.Id, x.ContractData["Name"], x.ContractData["Players"]))));
}
```

## Query
To do a query, you need the client library. After that you can use the MongoDB query language to send queries. The supported operations are:
```
$not - Inverses the nested query
$and - Combines multiple queries and ensures all are true
$or - Combines multiple queries and ensures at least one is true
$exists - Ensures a field exists
$text - Does a text search
$regex - Does a regex text search
$in - Ensures a field is included in a specific query list
$nin - Ensures a field is not included in a specific query list
$eq - Ensures a field is equal to a specific value
$ne - Ensures a field is not equal to a specific value
$gt - Ensures a field is greater than a specific value
$gte - Ensures a field is greater than or equal to a specific value
$lt - Ensures a field is less than a specific value
$lte - Ensures a field is less than or equal to a specific value
```

An example query can look like this:

```json
{
    "$and": [
        {
            "Players": {
                "$gte": 20
            }
        },
        {
            "$text": {
                "$search": "Hello world",
                "$caseSensitive": true
            }
        }
    ]
}
```

For more info, you can read up on the MongoDB query language.

This will find all the servers that has players greater than or equal to ``20`` AND has a text field that matches ``Hello world`` as a text query. Queries can be applied to ANY custom fields such as ELO or whatever field you would like.

## Contracts
ServerLists can have custom data, such as the amount of players, server name or any other field you would like. This is enforced with a contract. A contract is made of multiple contract definitions, each definition being a contract for its own field. Each definition has the following properties:

#### Name
This is the name of the contract. Such as "Players".

#### Required
Sets whether or not the field is required for all servers. Servers not setting this field, with the correct type will not be added.

#### Type
This is the field type, it can be one of the following:

```
Int8 - C# sbyte
Int16 - C# short
Int32 - C# int
Int64 - C# long
UInt8 - C# byte
UInt16 - C# ushort
UInt32 - C# uint
UInt64 - C# ulong
String - C# string
Buffer - C# byte[]
Guid - C# Guid
```