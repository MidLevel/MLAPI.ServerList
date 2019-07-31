using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using MLAPI.ServerList.Shared;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace MLAPI.ServerList.Server
{
    public static class Program
    {
        private static readonly Dictionary<ulong, ContractDefinition> contracts = new Dictionary<ulong, ContractDefinition>();
        private static readonly ConcurrentQueue<byte[]> buffers = new ConcurrentQueue<byte[]>();
        private static MongoClient mongoClient;

        private static List<ServerModel> localModels = new List<ServerModel>();

        // Checks if two contract definitions are compatible with each other
        private static bool IsCompatible(ContractDefinition[] v1, ContractDefinition[] v2)
        {
            // Contract conflict validation
            for (int i = 0; i < v1.Length; i++)
            {
                bool found = false;

                for (int j = 0; j < v2.Length; j++)
                {
                    if (v1[i].Name == v2[j].Name)
                    {
                        if (v1[i].Type != v2[j].Type)
                        {
                            return false;
                        }

                        found = true;
                    }
                }

                if (v1[i].Required && !found)
                {
                    // If required, fail if we dont find.
                    return false;
                }
            }


            // Contract conflict validation
            for (int i = 0; i < v2.Length; i++)
            {
                bool found = false;

                for (int j = 0; j < v1.Length; j++)
                {
                    if (v2[i].Name == v1[j].Name)
                    {
                        if (v2[i].Type != v1[j].Type)
                        {
                            return false;
                        }

                        found = true;
                    }
                }

                if (v2[i].Required && !found)
                {
                    // If required, fail if we dont find.
                    return false;
                }
            }

            return true;
        }

        private static bool FilterLocalServers(List<JToken> tokens, ServerModel serverModel)
        {
            string name = null;

            foreach (var child in tokens)
            {
                if (child.Type == JTokenType.Property)
                {
                    JProperty value = (JProperty)child;
                    name = value.Name;
                }

                switch (name)
                {
                    case "$not":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                List<JToken> children = child.Values().ToList();

                                return !FilterLocalServers(new List<JToken>() { children.First() }, serverModel);
                            }
                        }
                        break;
                    case "$and":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                List<JToken> children = child.Values().ToList();

                                return children.AsParallel().All(x => FilterLocalServers(new List<JToken>() { x }, serverModel));
                            }
                        }
                        break;
                    case "$or":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                List<JToken> children = child.Values().ToList();

                                return children.AsParallel().Any(x => FilterLocalServers(new List<JToken>() { x }, serverModel));
                            }
                        }
                        break;
                    case "$eq":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                string propertyName = ((JProperty)child.Parent.Parent).Name;

                                if (serverModel.ContractData.ContainsKey(propertyName))
                                {
                                    switch (child.Values().First().Type)
                                    {
                                        case JTokenType.Integer:
                                            return serverModel.ContractData[propertyName] is long && (long)serverModel.ContractData[propertyName] == child.Values().Values<long>().First();
                                        case JTokenType.Float:
                                            return serverModel.ContractData[propertyName] is float && Math.Abs((float)serverModel.ContractData[propertyName] - child.Values().Values<float>().First()) < 0.0001;
                                        case JTokenType.String:
                                            return serverModel.ContractData[propertyName] is string && (string)serverModel.ContractData[propertyName] == child.Values().Values<string>().First();
                                        case JTokenType.Boolean:
                                            return serverModel.ContractData[propertyName] is bool && (bool)serverModel.ContractData[propertyName] == child.Values().Values<bool>().First();
                                        case JTokenType.Date:
                                            return serverModel.ContractData[propertyName] is DateTime && (DateTime)serverModel.ContractData[propertyName] == child.Values().Values<DateTime>().First();
                                        case JTokenType.Bytes:
                                            return serverModel.ContractData[propertyName] is byte[] && ((byte[])serverModel.ContractData[propertyName]).SequenceEqual(child.Values().Values<byte[]>().First());
                                        case JTokenType.Guid:
                                            return serverModel.ContractData[propertyName] is Guid && (Guid)serverModel.ContractData[propertyName] == child.Values().Values<Guid>().First();
                                        case JTokenType.Uri:
                                            return serverModel.ContractData[propertyName] is Uri && (Uri)serverModel.ContractData[propertyName] == child.Values().Values<Uri>().First();
                                        case JTokenType.TimeSpan:
                                            return serverModel.ContractData[propertyName] is TimeSpan && (TimeSpan)serverModel.ContractData[propertyName] == child.Values().Values<TimeSpan>().First();
                                    }
                                }
                            }
                        }
                        break;
                    case "$regex":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                string propertyName = ((JProperty)child.Parent.Parent).Name;


                                if (serverModel.ContractData.ContainsKey(propertyName) && serverModel.ContractData[propertyName] is string)
                                {
                                    switch (child.Values().First().Type)
                                    {
                                        case JTokenType.String:
                                            {
                                                return Regex.IsMatch((string)serverModel.ContractData[propertyName], child.Values().Values<string>().First());
                                            }
                                    }
                                }
                            }
                        }
                        break;
                    case "$gt":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                string propertyName = ((JProperty)child.Parent.Parent).Name;

                                if (serverModel.ContractData.ContainsKey(propertyName))
                                {
                                    switch (child.Values().First().Type)
                                    {
                                        case JTokenType.Integer:
                                            return serverModel.ContractData[propertyName] is long && (long)serverModel.ContractData[propertyName] > child.Values().Values<long>().First();
                                        case JTokenType.Float:
                                            return serverModel.ContractData[propertyName] is float && (float)serverModel.ContractData[propertyName] > child.Values().Values<float>().First();
                                        case JTokenType.Date:
                                            return serverModel.ContractData[propertyName] is DateTime && (DateTime)serverModel.ContractData[propertyName] > child.Values().Values<DateTime>().First();
                                        case JTokenType.TimeSpan:
                                            return serverModel.ContractData[propertyName] is TimeSpan && (TimeSpan)serverModel.ContractData[propertyName] > child.Values().Values<TimeSpan>().First();
                                    }
                                }
                            }
                        }
                        break;
                    case "$gte":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                string propertyName = ((JProperty)child.Parent.Parent).Name;

                                if (serverModel.ContractData.ContainsKey(propertyName))
                                {
                                    switch (child.Values().First().Type)
                                    {
                                        case JTokenType.Integer:
                                            return serverModel.ContractData[propertyName] is long && (long)serverModel.ContractData[propertyName] >= child.Values().Values<long>().First();
                                        case JTokenType.Float:
                                            return serverModel.ContractData[propertyName] is float && (float)serverModel.ContractData[propertyName] >= child.Values().Values<float>().First();
                                        case JTokenType.Date:
                                            return serverModel.ContractData[propertyName] is DateTime && (DateTime)serverModel.ContractData[propertyName] >= child.Values().Values<DateTime>().First();
                                        case JTokenType.TimeSpan:
                                            return serverModel.ContractData[propertyName] is TimeSpan && (TimeSpan)serverModel.ContractData[propertyName] >= child.Values().Values<TimeSpan>().First();
                                    }
                                }
                            }
                        }
                        break;
                    case "$lt":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                string propertyName = ((JProperty)child.Parent.Parent).Name;

                                if (serverModel.ContractData.ContainsKey(propertyName))
                                {
                                    switch (child.Values().First().Type)
                                    {
                                        case JTokenType.Integer:
                                            return serverModel.ContractData[propertyName] is long && (long)serverModel.ContractData[propertyName] < child.Values().Values<long>().First();
                                        case JTokenType.Float:
                                            return serverModel.ContractData[propertyName] is float && (float)serverModel.ContractData[propertyName] < child.Values().Values<float>().First();
                                        case JTokenType.Date:
                                            return serverModel.ContractData[propertyName] is DateTime && (DateTime)serverModel.ContractData[propertyName] < child.Values().Values<DateTime>().First();
                                        case JTokenType.TimeSpan:
                                            return serverModel.ContractData[propertyName] is TimeSpan && (TimeSpan)serverModel.ContractData[propertyName] < child.Values().Values<TimeSpan>().First();
                                    }
                                }
                            }
                        }
                        break;
                    case "$lte":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                string propertyName = ((JProperty)child.Parent.Parent).Name;

                                if (serverModel.ContractData.ContainsKey(propertyName))
                                {
                                    switch (child.Values().First().Type)
                                    {
                                        case JTokenType.Integer:
                                            return serverModel.ContractData[propertyName] is long && (long)serverModel.ContractData[propertyName] <= child.Values().Values<long>().First();
                                        case JTokenType.Float:
                                            return serverModel.ContractData[propertyName] is float && (float)serverModel.ContractData[propertyName] <= child.Values().Values<float>().First();
                                        case JTokenType.Date:
                                            return serverModel.ContractData[propertyName] is DateTime && (DateTime)serverModel.ContractData[propertyName] <= child.Values().Values<DateTime>().First();
                                        case JTokenType.TimeSpan:
                                            return serverModel.ContractData[propertyName] is TimeSpan && (TimeSpan)serverModel.ContractData[propertyName] <= child.Values().Values<TimeSpan>().First();
                                    }
                                }
                            }
                        }
                        break;
                    default:
                        {
                            if (child.Type == JTokenType.Property || child.Type == JTokenType.Object)
                            {
                                return FilterLocalServers(child.Children().ToList(), serverModel);
                            }
                        }
                        break;
                }
            }

            return true;
        }

        private static FilterDefinition<ServerModel> CreateFilter(List<JToken> tokens)
        {
            string name = null;

            foreach (var child in tokens)
            {
                if (child.Type == JTokenType.Property)
                {
                    JProperty value = (JProperty)child;
                    name = value.Name;
                }

                switch (name)
                {
                    case "$not":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                return Builders<ServerModel>.Filter.Not(CreateFilter(new List<JToken>() { child.Values().First() }));
                            }
                        }
                        break;
                    case "$and":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                List<JToken> children = child.Values().ToList();
                                List<FilterDefinition<ServerModel>> childFilters = new List<FilterDefinition<ServerModel>>();

                                for (int i = 0; i < children.Count; i++)
                                {
                                    childFilters.Add(CreateFilter(new List<JToken>() { children[i] }));
                                }

                                return Builders<ServerModel>.Filter.And(childFilters);
                            }
                        }
                        break;
                    case "$or":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                List<JToken> children = child.Values().ToList();
                                List<FilterDefinition<ServerModel>> childFilters = new List<FilterDefinition<ServerModel>>();

                                for (int i = 0; i < children.Count; i++)
                                {
                                    childFilters.Add(CreateFilter(new List<JToken>() { children[i] }));
                                }

                                return Builders<ServerModel>.Filter.Or(childFilters);
                            }
                        }
                        break;
                    case "$eq":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                switch (child.Values().First().Type)
                                {
                                    case JTokenType.Integer:
                                        return Builders<ServerModel>.Filter.Eq(((JProperty)child.Parent.Parent).Name, child.Values().Values<long>().First());
                                    case JTokenType.Float:
                                        return Builders<ServerModel>.Filter.Eq(((JProperty)child.Parent.Parent).Name, child.Values().Values<float>().First());
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Eq(((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    case JTokenType.Boolean:
                                        return Builders<ServerModel>.Filter.Eq(((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>().First());
                                    case JTokenType.Date:
                                        return Builders<ServerModel>.Filter.Eq(((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>().First());
                                    case JTokenType.Bytes:
                                        return Builders<ServerModel>.Filter.Eq(((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>().First());
                                    case JTokenType.Guid:
                                        return Builders<ServerModel>.Filter.Eq(((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>().First());
                                    case JTokenType.Uri:
                                        return Builders<ServerModel>.Filter.Eq(((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>().First());
                                    case JTokenType.TimeSpan:
                                        return Builders<ServerModel>.Filter.Eq(((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>().First());
                                }
                            }
                        }
                        break;
                    case "$regex":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                switch (child.Values().First().Type)
                                {
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Regex(((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                }
                            }
                        }
                        break;
                    case "$gt":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                switch (child.Values().First().Type)
                                {
                                    case JTokenType.Integer:
                                        return Builders<ServerModel>.Filter.Gt(((JProperty)child.Parent.Parent).Name, child.Values().Values<long>().First());
                                    case JTokenType.Float:
                                        return Builders<ServerModel>.Filter.Gt(((JProperty)child.Parent.Parent).Name, child.Values().Values<float>().First());
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Gt(((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    case JTokenType.Boolean:
                                        return Builders<ServerModel>.Filter.Gt(((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>().First());
                                    case JTokenType.Date:
                                        return Builders<ServerModel>.Filter.Gt(((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>().First());
                                    case JTokenType.Bytes:
                                        return Builders<ServerModel>.Filter.Gt(((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>().First());
                                    case JTokenType.Guid:
                                        return Builders<ServerModel>.Filter.Gt(((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>().First());
                                    case JTokenType.Uri:
                                        return Builders<ServerModel>.Filter.Gt(((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>().First());
                                    case JTokenType.TimeSpan:
                                        return Builders<ServerModel>.Filter.Gt(((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>().First());
                                }
                            }
                        }
                        break;
                    case "$gte":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                switch (child.Values().First().Type)
                                {
                                    case JTokenType.Integer:
                                        return Builders<ServerModel>.Filter.Gte(((JProperty)child.Parent.Parent).Name, child.Values().Values<long>().First());
                                    case JTokenType.Float:
                                        return Builders<ServerModel>.Filter.Gte(((JProperty)child.Parent.Parent).Name, child.Values().Values<float>().First());
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Gte(((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    case JTokenType.Boolean:
                                        return Builders<ServerModel>.Filter.Gte(((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>().First());
                                    case JTokenType.Date:
                                        return Builders<ServerModel>.Filter.Gte(((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>().First());
                                    case JTokenType.Bytes:
                                        return Builders<ServerModel>.Filter.Gte(((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>().First());
                                    case JTokenType.Guid:
                                        return Builders<ServerModel>.Filter.Gte(((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>().First());
                                    case JTokenType.Uri:
                                        return Builders<ServerModel>.Filter.Gte(((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>().First());
                                    case JTokenType.TimeSpan:
                                        return Builders<ServerModel>.Filter.Gte(((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>().First());
                                }
                            }
                        }
                        break;
                    case "$lt":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                switch (child.Values().First().Type)
                                {
                                    case JTokenType.Integer:
                                        return Builders<ServerModel>.Filter.Lt(((JProperty)child.Parent.Parent).Name, child.Values().Values<int>().First());
                                    case JTokenType.Float:
                                        return Builders<ServerModel>.Filter.Lt(((JProperty)child.Parent.Parent).Name, child.Values().Values<float>().First());
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Lt(((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    case JTokenType.Boolean:
                                        return Builders<ServerModel>.Filter.Lt(((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>().First());
                                    case JTokenType.Date:
                                        return Builders<ServerModel>.Filter.Lt(((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>().First());
                                    case JTokenType.Bytes:
                                        return Builders<ServerModel>.Filter.Lt(((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>().First());
                                    case JTokenType.Guid:
                                        return Builders<ServerModel>.Filter.Lt(((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>().First());
                                    case JTokenType.Uri:
                                        return Builders<ServerModel>.Filter.Lt(((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>().First());
                                    case JTokenType.TimeSpan:
                                        return Builders<ServerModel>.Filter.Lt(((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>().First());
                                }
                            }
                        }
                        break;
                    case "$lte":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                switch (child.Values().First().Type)
                                {
                                    case JTokenType.Integer:
                                        return Builders<ServerModel>.Filter.Lte(((JProperty)child.Parent.Parent).Name, child.Values().Values<long>().First());
                                    case JTokenType.Float:
                                        return Builders<ServerModel>.Filter.Lte(((JProperty)child.Parent.Parent).Name, child.Values().Values<float>().First());
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Lte(((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    case JTokenType.Boolean:
                                        return Builders<ServerModel>.Filter.Lte(((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>().First());
                                    case JTokenType.Date:
                                        return Builders<ServerModel>.Filter.Lte(((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>().First());
                                    case JTokenType.Bytes:
                                        return Builders<ServerModel>.Filter.Lte(((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>().First());
                                    case JTokenType.Guid:
                                        return Builders<ServerModel>.Filter.Lte(((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>().First());
                                    case JTokenType.Uri:
                                        return Builders<ServerModel>.Filter.Lte(((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>().First());
                                    case JTokenType.TimeSpan:
                                        return Builders<ServerModel>.Filter.Lte(((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>().First());
                                }
                            }
                        }
                        break;
                    default:
                        {
                            if (child.Type == JTokenType.Property || child.Type == JTokenType.Object)
                            {
                                return CreateFilter(child.Children().ToList());
                            }
                        }
                        break;
                }
            }

            return null;
        }

        public static void Main(string[] args)
        {
            Console.WriteLine("Starting server...");

            string currentPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            string configPath = Path.Combine(currentPath, "config.json");

            Configuration configuration = null;

            if (File.Exists(configPath) && false)
            {
                try
                {
                    // Parse configuration
                    configuration = JsonConvert.DeserializeObject<Configuration>(File.ReadAllText(configPath));
                }
                catch
                {

                }
            }

            // Create configuration
            if (configuration == null)
            {
                configuration = new Configuration();

                File.WriteAllText(configPath, JsonConvert.SerializeObject(configuration));
            }

            // Hash contract definitions
            for (int i = 0; i < configuration.ServerContract.Length; i++)
            {
                contracts.Add(configuration.ServerContract[i].Name.GetStableHash64(), configuration.ServerContract[i]);
            }

            if (configuration.UseMongo)
            {
                mongoClient = new MongoClient(configuration.MongoConnection);
            }

            Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            socket.Bind(new IPEndPoint(IPAddress.Any, configuration.Port));

            while (true)
            {
                if (!buffers.TryDequeue(out byte[] buffer))
                {
                    buffer = new byte[configuration.BufferSize];
                }

                EndPoint endpoint = new IPEndPoint(IPAddress.Any, 0);

                int size = socket.ReceiveFrom(buffer, ref endpoint);

                if (size > 0)
                {
                    Task.Run(async () =>
                    {
                        try
                        {
                            using (MemoryStream stream = new MemoryStream(buffer, 0, size))
                            {
                                using (BinaryReader reader = new BinaryReader(stream))
                                {
                                    byte messageType = reader.ReadByte();

                                    if (messageType == (byte)MessageType.RegisterServer)
                                    {
                                        // Parse contract
                                        Dictionary<string, ContractValue> contractValues = new Dictionary<string, ContractValue>();
                                        uint valueCount = reader.ReadUInt32();

                                        for (uint i = 0; i < valueCount; i++)
                                        {
                                            ulong nameHash = reader.ReadUInt64();

                                            if (contracts.TryGetValue(nameHash, out ContractDefinition definition))
                                            {
                                                object boxedValue = null;

                                                switch (definition.Type)
                                                {
                                                    case ContractType.Int8:
                                                        boxedValue = (long)reader.ReadSByte();
                                                        break;
                                                    case ContractType.Int16:
                                                        boxedValue = (long)reader.ReadInt16();
                                                        break;
                                                    case ContractType.Int32:
                                                        boxedValue = (long)reader.ReadInt32();
                                                        break;
                                                    case ContractType.Int64:
                                                        boxedValue = (long)reader.ReadInt32();
                                                        break;
                                                    case ContractType.UInt8:
                                                        boxedValue = (long)reader.ReadByte();
                                                        break;
                                                    case ContractType.UInt16:
                                                        boxedValue = (long)reader.ReadUInt16();
                                                        break;
                                                    case ContractType.UInt32:
                                                        boxedValue = (long)reader.ReadUInt32();
                                                        break;
                                                    case ContractType.UInt64:
                                                        boxedValue = (long)reader.ReadUInt64();
                                                        break;
                                                    case ContractType.String:
                                                        boxedValue = reader.ReadString();
                                                        break;
                                                    case ContractType.Buffer:
                                                        boxedValue = reader.ReadBytes(reader.ReadInt32());
                                                        break;
                                                    case ContractType.Guid:
                                                        boxedValue = new Guid(reader.ReadBytes(16));
                                                        break;
                                                }

                                                if (boxedValue != null)
                                                {
                                                    contractValues.Add(definition.Name, new ContractValue()
                                                    {
                                                        Definition = definition,
                                                        Value = boxedValue
                                                    });
                                                }
                                            }
                                        }

                                        // Contract validation, ensure all REQUIRED fields are present
                                        for (int i = 0; i < configuration.ServerContract.Length; i++)
                                        {
                                            if (configuration.ServerContract[i].Required)
                                            {
                                                if (!contractValues.TryGetValue(configuration.ServerContract[i].Name, out ContractValue contractValue) || contractValue.Definition.Type != configuration.ServerContract[i].Type)
                                                {
                                                    // Failure, contract did not match
                                                    return;
                                                }
                                            }
                                        }

                                        List<ContractValue> validatedValues = new List<ContractValue>();

                                        // Remove all fields not part of contract
                                        for (int i = 0; i < configuration.ServerContract.Length; i++)
                                        {
                                            if (contractValues.TryGetValue(configuration.ServerContract[i].Name, out ContractValue contractValue) && contractValue.Definition.Type == configuration.ServerContract[i].Type)
                                            {
                                                validatedValues.Add(contractValue);
                                            }
                                        }

                                        // Create model for DB
                                        ServerModel server = new ServerModel()
                                        {
                                            Id = Guid.NewGuid(),
                                            LastPingTime = DateTime.Now,
                                            Address = ((IPEndPoint)endpoint).Address.MapToIPv6(),
                                            ContractData = new Dictionary<string, object>()
                                        };

                                        // Add contract values to model
                                        for (int i = 0; i < validatedValues.Count; i++)
                                        {
                                            server.ContractData.Add(validatedValues[i].Definition.Name, validatedValues[i].Value);
                                        }

                                        if (configuration.UseMongo)
                                        {
                                            // Insert model to DB
                                            await mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").InsertOneAsync(server);
                                        }
                                        else
                                        {
                                            localModels.Add(server);
                                        }
                                    }
                                    else if (messageType == (byte)MessageType.Query)
                                    {
                                        string query = reader.ReadString();
                                        JObject parsedQuery = JObject.Parse(query);

                                        List<ServerModel> serverModel = null;

                                        if (configuration.UseMongo)
                                        {
                                            FilterDefinition<ServerModel> filter = CreateFilter(new List<JToken>() { parsedQuery });

                                            serverModel = await (await mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").FindAsync(filter)).ToListAsync();
                                        }
                                        else
                                        {
                                            serverModel = localModels.AsParallel().Where(x => FilterLocalServers(new List<JToken>() { parsedQuery }, x)).ToList();
                                        }

                                        using (MemoryStream sendStream = new MemoryStream())
                                        {
                                            using (BinaryWriter writer = new BinaryWriter(sendStream))
                                            {
                                                writer.Write(serverModel.Count);

                                                for (int i = 0; i < serverModel.Count; i++)
                                                {
                                                    writer.Write(serverModel[i].Id.ToByteArray());
                                                    writer.Write(serverModel[i].Address.MapToIPv6().GetAddressBytes());
                                                    writer.Write(serverModel[i].LastPingTime.ToBinary());
                                                    writer.Write(serverModel[i].ContractData.Count);

                                                    foreach (KeyValuePair<string, object> pair in serverModel[i].ContractData)
                                                    {
                                                        writer.Write(pair.Key);
                                                        // TODO: Fix writing
                                                        //writer.Write(pair.Value);
                                                    }
                                                }
                                            }

                                            socket.SendTo(sendStream.ToArray(), endpoint);
                                        }
                                    }
                                    else if (messageType == (byte)MessageType.ServerAlive)
                                    {
                                        Guid guid = new Guid(reader.ReadBytes(16));

                                        if (configuration.UseMongo)
                                        {
                                            // Find and validate address ownership
                                            FilterDefinition<ServerModel> filter = Builders<ServerModel>.Filter.And(Builders<ServerModel>.Filter.Eq(x => x.Address, ((IPEndPoint)endpoint).Address.MapToIPv6()), Builders<ServerModel>.Filter.Eq(x => x.Id, guid));
                                            // Create update
                                            UpdateDefinition<ServerModel> update = Builders<ServerModel>.Update.Set(x => x.LastPingTime, DateTime.UtcNow);

                                            // Execute
                                            await mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").FindOneAndUpdateAsync(filter, update);
                                        }
                                        else
                                        {
                                            ServerModel model = localModels.Find(x => x.Address == ((IPEndPoint)endpoint).Address.MapToIPv6() && x.Id == guid);
                                            model.LastPingTime = DateTime.UtcNow;
                                        }
                                    }
                                    else if (messageType == (byte)MessageType.RemoveServer)
                                    {
                                        Guid guid = new Guid(reader.ReadBytes(16));

                                        if (configuration.UseMongo)
                                        {
                                            // Find and validate address ownership
                                            FilterDefinition<ServerModel> filter = Builders<ServerModel>.Filter.And(Builders<ServerModel>.Filter.Eq(x => x.Address, ((IPEndPoint)endpoint).Address.MapToIPv6()), Builders<ServerModel>.Filter.Eq(x => x.Id, guid));

                                            // Execute
                                            await mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").FindOneAndDeleteAsync(filter);
                                        }
                                        else
                                        {
                                            localModels.RemoveAll(x => x.Id == guid && x.Address == ((IPEndPoint)endpoint).Address.MapToIPv6());
                                        }
                                    }
                                    else if (messageType == (byte)MessageType.UpdateServer)
                                    {
                                        Guid guid = new Guid(reader.ReadBytes(16));

                                        ServerModel result = null;

                                        if (configuration.UseMongo)
                                        {
                                            result = await mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").Find(x => x.Id == guid && x.Address == ((IPEndPoint)endpoint).Address.MapToIPv6()).FirstOrDefaultAsync();
                                        }
                                        else
                                        {
                                            result = localModels.Find(x => x.Id == guid && x.Address == ((IPEndPoint)endpoint).Address.MapToIPv6());
                                        }

                                        if (result != null)
                                        {
                                            // Parse contract
                                            Dictionary<string, ContractValue> contractValues = new Dictionary<string, ContractValue>();
                                            uint valueCount = reader.ReadUInt32();

                                            for (uint i = 0; i < valueCount; i++)
                                            {
                                                ulong nameHash = reader.ReadUInt64();

                                                if (contracts.TryGetValue(nameHash, out ContractDefinition definition))
                                                {
                                                    object boxedValue = null;

                                                    switch (definition.Type)
                                                    {
                                                        case ContractType.Int8:
                                                            boxedValue = (long)reader.ReadSByte();
                                                            break;
                                                        case ContractType.Int16:
                                                            boxedValue = (long)reader.ReadInt16();
                                                            break;
                                                        case ContractType.Int32:
                                                            boxedValue = (long)reader.ReadInt32();
                                                            break;
                                                        case ContractType.Int64:
                                                            boxedValue = (long)reader.ReadInt32();
                                                            break;
                                                        case ContractType.UInt8:
                                                            boxedValue = (long)reader.ReadByte();
                                                            break;
                                                        case ContractType.UInt16:
                                                            boxedValue = (long)reader.ReadUInt16();
                                                            break;
                                                        case ContractType.UInt32:
                                                            boxedValue = (long)reader.ReadUInt32();
                                                            break;
                                                        case ContractType.UInt64:
                                                            boxedValue = (long)reader.ReadUInt64();
                                                            break;
                                                        case ContractType.String:
                                                            boxedValue = reader.ReadString();
                                                            break;
                                                        case ContractType.Buffer:
                                                            boxedValue = reader.ReadBytes(reader.ReadInt32());
                                                            break;
                                                        case ContractType.Guid:
                                                            boxedValue = new Guid(reader.ReadBytes(16));
                                                            break;
                                                    }

                                                    if (boxedValue != null)
                                                    {
                                                        contractValues.Add(definition.Name, new ContractValue()
                                                        {
                                                            Definition = definition,
                                                            Value = boxedValue
                                                        });
                                                    }
                                                }
                                            }

                                            // Contract validation, ensure all REQUIRED fields are present
                                            for (int i = 0; i < configuration.ServerContract.Length; i++)
                                            {
                                                if (configuration.ServerContract[i].Required)
                                                {
                                                    if (!contractValues.TryGetValue(configuration.ServerContract[i].Name, out ContractValue contractValue) || contractValue.Definition.Type != configuration.ServerContract[i].Type)
                                                    {
                                                        // Failure, contract did not match
                                                        return;
                                                    }
                                                }
                                            }

                                            List<ContractValue> validatedValues = new List<ContractValue>();

                                            // Remove all fields not part of contract
                                            for (int i = 0; i < configuration.ServerContract.Length; i++)
                                            {
                                                if (contractValues.TryGetValue(configuration.ServerContract[i].Name, out ContractValue contractValue) && contractValue.Definition.Type == configuration.ServerContract[i].Type)
                                                {
                                                    validatedValues.Add(contractValue);
                                                }
                                            }

                                            Dictionary<string, object> validatedLookupValues = new Dictionary<string, object>();

                                            // Add contract values to model
                                            for (int i = 0; i < validatedValues.Count; i++)
                                            {
                                                validatedLookupValues.Add(validatedValues[i].Definition.Name, validatedValues[i].Value);
                                            }

                                            if (configuration.UseMongo)
                                            {
                                                // Find and validate address ownership
                                                FilterDefinition<ServerModel> filter = Builders<ServerModel>.Filter.And(Builders<ServerModel>.Filter.Eq(x => x.Address, ((IPEndPoint)endpoint).Address.MapToIPv6()), Builders<ServerModel>.Filter.Eq(x => x.Id, guid));
                                                // Create update
                                                UpdateDefinition<ServerModel> update = Builders<ServerModel>.Update.Set(x => x.LastPingTime, DateTime.UtcNow).Set(x => x.ContractData, validatedLookupValues);

                                                // Insert model to DB
                                                await mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").FindOneAndUpdateAsync(filter, update);
                                            }
                                            else
                                            {
                                                ServerModel model = localModels.Find(x => x.Address == ((IPEndPoint)endpoint).Address.MapToIPv6() && x.Id == guid);
                                                model.LastPingTime = DateTime.UtcNow;
                                                model.ContractData = validatedLookupValues;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        finally
                        {
                            buffers.Enqueue(buffer);
                        }
                    });
                }
            }
        }
    }
}
