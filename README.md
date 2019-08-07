# MLAPI.ServerList
The MLAPI.ServerList is a server list designed for video games. It's built of one server application that can shard horizontally and a client library that can register servers and make queries. It's built to be high performance, feature rich and general purpose.

# Features
* Cross platform
* Search for servers with the MongoDB query language (sub and super set of the official language)
* Shards horizontally with MongoDB
* Can run without sharding in memory
* Custom data is contracted
* Custom data contracts can be changed and be backwards compatible


# TODO
* File database to reduce memory usage when not using shards
* TLS

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