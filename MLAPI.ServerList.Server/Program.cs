using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
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
        private static MongoClient mongoClient;
        private static Configuration configuration;

        private static List<ServerModel> localModels = new List<ServerModel>();

        private static bool FilterLocalServers(List<JToken> tokens, ServerModel serverModel)
        {
            string name = null;

            foreach (JToken child in tokens)
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
                    case "$ne":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                string propertyName = ((JProperty)child.Parent.Parent).Name;

                                if (serverModel.ContractData.ContainsKey(propertyName))
                                {
                                    switch (child.Values().First().Type)
                                    {
                                        case JTokenType.Integer:
                                            return !(serverModel.ContractData[propertyName] is long) || (long)serverModel.ContractData[propertyName] != child.Values().Values<long>().First();
                                        case JTokenType.Float:
                                            return (serverModel.ContractData[propertyName] is float) || Math.Abs((float)serverModel.ContractData[propertyName] - child.Values().Values<float>().First()) >= 0.0001;
                                        case JTokenType.String:
                                            return (serverModel.ContractData[propertyName] is string) || (string)serverModel.ContractData[propertyName] != child.Values().Values<string>().First();
                                        case JTokenType.Boolean:
                                            return (serverModel.ContractData[propertyName] is bool) || (bool)serverModel.ContractData[propertyName] != child.Values().Values<bool>().First();
                                        case JTokenType.Date:
                                            return (serverModel.ContractData[propertyName] is DateTime) || (DateTime)serverModel.ContractData[propertyName] != child.Values().Values<DateTime>().First();
                                        case JTokenType.Bytes:
                                            return (serverModel.ContractData[propertyName] is byte[]) || !((byte[])serverModel.ContractData[propertyName]).SequenceEqual(child.Values().Values<byte[]>().First());
                                        case JTokenType.Guid:
                                            return (serverModel.ContractData[propertyName] is Guid) || (Guid)serverModel.ContractData[propertyName] != child.Values().Values<Guid>().First();
                                        case JTokenType.Uri:
                                            return (serverModel.ContractData[propertyName] is Uri) || (Uri)serverModel.ContractData[propertyName] != child.Values().Values<Uri>().First();
                                        case JTokenType.TimeSpan:
                                            return (serverModel.ContractData[propertyName] is TimeSpan) || (TimeSpan)serverModel.ContractData[propertyName] != child.Values().Values<TimeSpan>().First();
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
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<long>().First());
                                    case JTokenType.Float:
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<float>().First());
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    case JTokenType.Boolean:
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>().First());
                                    case JTokenType.Date:
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>().First());
                                    case JTokenType.Bytes:
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>().First());
                                    case JTokenType.Guid:
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>().First());
                                    case JTokenType.Uri:
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>().First());
                                    case JTokenType.TimeSpan:
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>().First());
                                }
                            }
                        }
                        break;
                    case "$ne":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                switch (child.Values().First().Type)
                                {
                                    case JTokenType.Integer:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<long>().First());
                                    case JTokenType.Float:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<float>().First());
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    case JTokenType.Boolean:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>().First());
                                    case JTokenType.Date:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>().First());
                                    case JTokenType.Bytes:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>().First());
                                    case JTokenType.Guid:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>().First());
                                    case JTokenType.Uri:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>().First());
                                    case JTokenType.TimeSpan:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>().First());
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
                                        return Builders<ServerModel>.Filter.Regex("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
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
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<long>().First());
                                    case JTokenType.Float:
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<float>().First());
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    case JTokenType.Boolean:
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>().First());
                                    case JTokenType.Date:
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>().First());
                                    case JTokenType.Bytes:
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>().First());
                                    case JTokenType.Guid:
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>().First());
                                    case JTokenType.Uri:
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>().First());
                                    case JTokenType.TimeSpan:
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>().First());
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
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<long>().First());
                                    case JTokenType.Float:
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<float>().First());
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    case JTokenType.Boolean:
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>().First());
                                    case JTokenType.Date:
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>().First());
                                    case JTokenType.Bytes:
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>().First());
                                    case JTokenType.Guid:
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>().First());
                                    case JTokenType.Uri:
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>().First());
                                    case JTokenType.TimeSpan:
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>().First());
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
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<long>().First());
                                    case JTokenType.Float:
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<float>().First());
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    case JTokenType.Boolean:
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>().First());
                                    case JTokenType.Date:
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>().First());
                                    case JTokenType.Bytes:
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>().First());
                                    case JTokenType.Guid:
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>().First());
                                    case JTokenType.Uri:
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>().First());
                                    case JTokenType.TimeSpan:
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>().First());
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
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<long>().First());
                                    case JTokenType.Float:
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<float>().First());
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    case JTokenType.Boolean:
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>().First());
                                    case JTokenType.Date:
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>().First());
                                    case JTokenType.Bytes:
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>().First());
                                    case JTokenType.Guid:
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>().First());
                                    case JTokenType.Uri:
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>().First());
                                    case JTokenType.TimeSpan:
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>().First());
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

            Task.Run(async () =>
            {
                TcpListener listener = new TcpListener(IPAddress.Parse(configuration.ListenAddress), configuration.Port);
                listener.Start();

                while (true)
                {
                    TcpClient client = await listener.AcceptTcpClientAsync();

                    new Thread(() => HandleNewClient(client).Wait()).Start();
                }
            }).Wait();
        }

        private static async Task HandleNewClient(TcpClient client)
        {
            while (client.Connected)
            {
                await HandleIncomingMessage(client);
            }
        }

        private static async Task HandleIncomingMessage(TcpClient client)
        {
            try
            {
                using (BinaryReader reader = new BinaryReader(client.GetStream(), Encoding.UTF8, true))
                {
                    byte messageType = reader.ReadByte();

                    if (messageType == (byte)MessageType.RegisterServer)
                    {
                        Console.WriteLine("[Register] Started");

                        // Parse contract
                        Dictionary<string, ContractValue> contractValues = new Dictionary<string, ContractValue>();
                        int valueCount = reader.ReadInt32();

                        for (int i = 0; i < valueCount; i++)
                        {
                            ulong nameHash = reader.ReadUInt64();

                            ContractType type = (ContractType)reader.ReadByte();

                            if (contracts.TryGetValue(nameHash, out ContractDefinition definition) && definition.Type == type)
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
                                        boxedValue = new Guid(reader.ReadString());
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
                            else
                            {
                                switch (type)
                                {
                                    case ContractType.Int8:
                                        reader.ReadSByte();
                                        break;
                                    case ContractType.Int16:
                                        reader.ReadInt16();
                                        break;
                                    case ContractType.Int32:
                                        reader.ReadInt32();
                                        break;
                                    case ContractType.Int64:
                                        reader.ReadInt32();
                                        break;
                                    case ContractType.UInt8:
                                        reader.ReadByte();
                                        break;
                                    case ContractType.UInt16:
                                        reader.ReadUInt16();
                                        break;
                                    case ContractType.UInt32:
                                        reader.ReadUInt32();
                                        break;
                                    case ContractType.UInt64:
                                        reader.ReadUInt64();
                                        break;
                                    case ContractType.String:
                                        reader.ReadString();
                                        break;
                                    case ContractType.Buffer:
                                        reader.ReadBytes(reader.ReadInt32());
                                        break;
                                    case ContractType.Guid:
                                        reader.ReadString();
                                        break;
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
                                    using (BinaryWriter writer = new BinaryWriter(client.GetStream(), Encoding.UTF8, true))
                                    {
                                        writer.Write((byte)MessageType.RegisterAck);
                                        writer.Write(new Guid().ToString());
                                        writer.Write(false);
                                    }

                                    Console.WriteLine("[Register] Registrar broke contract. Missing required field \"" + configuration.ServerContract[i].Name + "\" of type " + configuration.ServerContract[i].Type);
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
                            Id = Guid.NewGuid().ToString(),
                            LastPingTime = DateTime.UtcNow,
                            Address = ((IPEndPoint)client.Client.RemoteEndPoint).Address.MapToIPv6(),
                            ContractData = new Dictionary<string, object>()
                        };

                        // Add contract values to model
                        for (int i = 0; i < validatedValues.Count; i++)
                        {
                            server.ContractData.Add(validatedValues[i].Definition.Name, validatedValues[i].Value);
                        }

                        if (configuration.VerbosePrints)
                        {
                            Console.WriteLine("[Register] Adding: " + JsonConvert.SerializeObject(server));
                        }
                        else
                        {
                            Console.WriteLine("[Register] Adding 1 server");
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

                        using (BinaryWriter writer = new BinaryWriter(client.GetStream(), Encoding.UTF8, true))
                        {
                            writer.Write((byte)MessageType.RegisterAck);
                            writer.Write(server.Id);
                            writer.Write(true);
                        }
                    }
                    else if (messageType == (byte)MessageType.Query)
                    {
                        DateTime startTime = DateTime.Now;
                        Console.WriteLine("[Query] Started");
                        string guid = reader.ReadString();
                        string query = reader.ReadString();
                        Console.WriteLine("[Query] Parsing");
                        JObject parsedQuery = JObject.Parse(query);

                        List<ServerModel> serverModel = null;

                        if (configuration.UseMongo)
                        {
                            Console.WriteLine("[Query] Creating mongo filter");
                            FilterDefinition<ServerModel> filter = Builders<ServerModel>.Filter.And(Builders<ServerModel>.Filter.Where(x => x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout)), CreateFilter(new List<JToken>() { parsedQuery }));

                            if (configuration.VerbosePrints)
                            {
                                Console.WriteLine("[Query] Executing mongo query \"" + mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").Find(filter) + "\"");
                            }
                            else
                            {
                                Console.WriteLine("[Query] Executing mongo query");
                            }

                            serverModel = await (await mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").FindAsync(filter)).ToListAsync();
                        }
                        else
                        {
                            Console.WriteLine("[Query] Querying local");
                            serverModel = localModels.AsParallel().Where(x => x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout) && FilterLocalServers(new List<JToken>() { parsedQuery }, x)).ToList();
                        }

                        Console.WriteLine("[Query] Found " + (serverModel == null ? 0 : serverModel.Count) + " results. Total query time: " + (DateTime.Now - startTime).TotalMilliseconds + " ms");

                        using (BinaryWriter writer = new BinaryWriter(client.GetStream(), Encoding.UTF8, true))
                        {
                            writer.Write((byte)MessageType.QueryResponse);
                            writer.Write(guid);
                            writer.Write(serverModel.Count);

                            for (int i = 0; i < serverModel.Count; i++)
                            {
                                writer.Write(serverModel[i].Id);
                                writer.Write(serverModel[i].Address.MapToIPv6().GetAddressBytes());
                                writer.Write(serverModel[i].LastPingTime.ToBinary());
                                writer.Write(serverModel[i].ContractData.Count);

                                foreach (KeyValuePair<string, object> pair in serverModel[i].ContractData)
                                {
                                    writer.Write(pair.Key);
                                    writer.Write((byte)contracts[pair.Key.GetStableHash64()].Type);

                                    switch (contracts[pair.Key.GetStableHash64()].Type)
                                    {
                                        case ContractType.Int8:
                                            writer.Write((sbyte)(long)pair.Value);
                                            break;
                                        case ContractType.Int16:
                                            writer.Write((short)(long)pair.Value);
                                            break;
                                        case ContractType.Int32:
                                            writer.Write((int)(long)pair.Value);
                                            break;
                                        case ContractType.Int64:
                                            writer.Write((long)pair.Value);
                                            break;
                                        case ContractType.UInt8:
                                            writer.Write((byte)(long)pair.Value);
                                            break;
                                        case ContractType.UInt16:
                                            writer.Write((ushort)(long)pair.Value);
                                            break;
                                        case ContractType.UInt32:
                                            writer.Write((uint)(long)pair.Value);
                                            break;
                                        case ContractType.UInt64:
                                            writer.Write((ulong)(long)pair.Value);
                                            break;
                                        case ContractType.String:
                                            writer.Write((string)pair.Value);
                                            break;
                                        case ContractType.Buffer:
                                            writer.Write(((byte[])pair.Value).Length);
                                            writer.Write((byte[])pair.Value);
                                            break;
                                        case ContractType.Guid:
                                            writer.Write(((Guid)pair.Value).ToString());
                                            break;
                                    }
                                }
                            }
                        }
                    }
                    else if (messageType == (byte)MessageType.ServerAlive)
                    {
                        Console.WriteLine("[Alive] Started");
                        Guid guid = new Guid(reader.ReadString());

                        if (configuration.UseMongo)
                        {
                            // Find and validate address ownership
                            FilterDefinition<ServerModel> filter = Builders<ServerModel>.Filter.And(Builders<ServerModel>.Filter.Where(x => x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout)), Builders<ServerModel>.Filter.Eq(x => x.Address, ((IPEndPoint)client.Client.RemoteEndPoint).Address.MapToIPv6()), Builders<ServerModel>.Filter.Eq(x => x.Id, guid.ToString()));
                            // Create update
                            UpdateDefinition<ServerModel> update = Builders<ServerModel>.Update.Set(x => x.LastPingTime, DateTime.UtcNow);

                            // Execute
                            await mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").FindOneAndUpdateAsync(filter, update);
                        }
                        else
                        {
                            ServerModel model = localModels.Find(x => x.Address.Equals(((IPEndPoint)client.Client.RemoteEndPoint).Address.MapToIPv6()) && x.Id == guid.ToString() && x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout));

                            if (model != null)
                            {
                                model.LastPingTime = DateTime.UtcNow;
                            }
                        }
                    }
                    else if (messageType == (byte)MessageType.RemoveServer)
                    {
                        Console.WriteLine("[Remove] Started");

                        Guid guid = new Guid(reader.ReadString());

                        ServerModel model = null;

                        if (configuration.UseMongo)
                        {
                            // Find and validate address ownership
                            FilterDefinition<ServerModel> filter = Builders<ServerModel>.Filter.And(Builders<ServerModel>.Filter.Where(x => x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout)), Builders<ServerModel>.Filter.Eq(x => x.Address, ((IPEndPoint)client.Client.RemoteEndPoint).Address.MapToIPv6()), Builders<ServerModel>.Filter.Eq(x => x.Id, guid.ToString()));

                            // Execute
                            model = await mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").FindOneAndDeleteAsync(filter);
                        }
                        else
                        {
                            model = localModels.Find(x => x.Id == guid.ToString() && x.Address.Equals(((IPEndPoint)client.Client.RemoteEndPoint).Address.MapToIPv6()));

                            if (model != null)
                            {
                                localModels.Remove(model);
                            }
                        }

                        if (model != null)
                        {
                            if (configuration.VerbosePrints)
                            {
                                Console.WriteLine("[Remove] Removed: " + JsonConvert.SerializeObject(model));
                            }
                            else
                            {
                                Console.WriteLine("[Remove] Removed 1 element");
                            }
                        }
                        else
                        {
                            Console.WriteLine("[Remove] Not found");
                        }
                    }
                    else if (messageType == (byte)MessageType.UpdateServer)
                    {
                        Console.WriteLine("[Update] Started");
                        Guid guid = new Guid(reader.ReadString());

                        ServerModel result = null;

                        if (configuration.UseMongo)
                        {
                            result = await mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").Find(x => x.Id == guid.ToString() && x.Address == ((IPEndPoint)client.Client.RemoteEndPoint).Address.MapToIPv6() && x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout)).FirstOrDefaultAsync();
                        }
                        else
                        {
                            result = localModels.Find(x => x.Id == guid.ToString() && x.Address.Equals(((IPEndPoint)client.Client.RemoteEndPoint).Address.MapToIPv6()) && x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout));
                        }

                        if (result != null)
                        {
                            // Parse contract
                            Dictionary<string, ContractValue> contractValues = new Dictionary<string, ContractValue>();
                            int valueCount = reader.ReadInt32();

                            for (int i = 0; i < valueCount; i++)
                            {
                                ulong nameHash = reader.ReadUInt64();

                                ContractType type = (ContractType)reader.ReadByte();

                                if (contracts.TryGetValue(nameHash, out ContractDefinition definition) && definition.Type == type)
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
                                            boxedValue = (long)reader.ReadInt64();
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
                                            boxedValue = new Guid(reader.ReadString());
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
                                else
                                {
                                    switch (type)
                                    {
                                        case ContractType.Int8:
                                            reader.ReadSByte();
                                            break;
                                        case ContractType.Int16:
                                            reader.ReadInt16();
                                            break;
                                        case ContractType.Int32:
                                            reader.ReadInt32();
                                            break;
                                        case ContractType.Int64:
                                            reader.ReadInt64();
                                            break;
                                        case ContractType.UInt8:
                                            reader.ReadByte();
                                            break;
                                        case ContractType.UInt16:
                                            reader.ReadUInt16();
                                            break;
                                        case ContractType.UInt32:
                                            reader.ReadUInt32();
                                            break;
                                        case ContractType.UInt64:
                                            reader.ReadUInt64();
                                            break;
                                        case ContractType.String:
                                            reader.ReadString();
                                            break;
                                        case ContractType.Buffer:
                                            reader.ReadBytes(reader.ReadInt32());
                                            break;
                                        case ContractType.Guid:
                                            reader.ReadString();
                                            break;
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
                                FilterDefinition<ServerModel> filter = Builders<ServerModel>.Filter.And(Builders<ServerModel>.Filter.Where(x => x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout)), Builders<ServerModel>.Filter.Eq(x => x.Address, ((IPEndPoint)client.Client.RemoteEndPoint).Address.MapToIPv6()), Builders<ServerModel>.Filter.Eq(x => x.Id, guid.ToString()));
                                // Create update
                                UpdateDefinition<ServerModel> update = Builders<ServerModel>.Update.Set(x => x.LastPingTime, DateTime.UtcNow).Set(x => x.ContractData, validatedLookupValues);

                                // Insert model to DB
                                await mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").FindOneAndUpdateAsync(filter, update);
                            }
                            else
                            {
                                ServerModel model = localModels.Find(x => x.Address.Equals(((IPEndPoint)client.Client.RemoteEndPoint).Address.MapToIPv6()) && x.Id == guid.ToString() && x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout));
                                model.LastPingTime = DateTime.UtcNow;
                                model.ContractData = validatedLookupValues;
                            }
                        }
                    }
                    else if (messageType == (byte)MessageType.ContractCheck)
                    {
                        Console.WriteLine("[ContractCheck] Started");

                        string guid = reader.ReadString();
                        int contractCount = reader.ReadInt32();

                        WeakContractDefinition[] remoteContracts = new WeakContractDefinition[contractCount];

                        for (int i = 0; i < contractCount; i++)
                        {
                            remoteContracts[i] = new WeakContractDefinition()
                            {
                                Name = reader.ReadString(),
                                Type = (ContractType)reader.ReadByte()
                            };
                        }

                        using (BinaryWriter writer = new BinaryWriter(client.GetStream(), Encoding.UTF8, true))
                        {
                            writer.Write((byte)MessageType.ContractResponse);
                            writer.Write(guid);
                            writer.Write(ContractDefinition.IsCompatible(remoteContracts, contracts.Select(x => x.Value).ToArray()));
                        }
                    }
                }
            }
            catch (IOException)
            {
                client.Close();
            }
        }
    }
}
