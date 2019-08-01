using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
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
        private static Dictionary<Socket, byte[]> receiveBuffers = new Dictionary<Socket, byte[]>();

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

                IndexKeysDefinition<ServerModel> indexDefinition = Builders<ServerModel>.IndexKeys.Ascending(x => x.LastPingTime);

                try
                {
                    mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").Indexes.DropOne("ServerExpirationIndex");
                }
                catch (MongoCommandException)
                {
                    // Index probably didnt exist
                }
                
                mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").Indexes.CreateOne(new CreateIndexModel<ServerModel>(indexDefinition, new CreateIndexOptions()
                {
                    Name = "ServerExpirationIndex",
                    ExpireAfter = TimeSpan.FromMilliseconds(configuration.CollectionExpiryDelay)
                }));
            }
            else
            {
                new Thread(() =>
                {
                    while (true)
                    {
                        localModels.RemoveAll(x => x != null && (DateTime.UtcNow - x.LastPingTime).TotalMilliseconds > configuration.CollectionExpiryDelay);

                        Thread.Sleep(5000);
                    }
                }).Start();
            }

            Socket listener = new Socket(IPAddress.Parse(configuration.ListenAddress).AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            listener.Bind(new IPEndPoint(IPAddress.Parse(configuration.ListenAddress), configuration.Port));
            listener.Listen(110);

            StartAccept(listener);

            Console.Read();
        }

        private static void StartAccept(Socket listener)
        {
            try
            {
                listener.BeginAccept((e) =>
                {
                    Socket socket = listener.EndAccept(e);

                    receiveBuffers[socket] = new byte[1024 * 8];

                    HandleData(socket, 0, 0, 2);

                    StartAccept(listener);
                }, null);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        // Position is the pos with the size, targetLength is the length INCLUDING length
        private static void HandleData(Socket socket, int readOffset, int position, int targetLength)
        {
            try
            {
                socket.BeginReceive(receiveBuffers[socket], readOffset, targetLength - position, SocketFlags.None, (e) =>
                {
                    int data = socket.EndReceive(e);

                    if (data <= 0)
                    {
                        socket.Close();
                        socket.Dispose();
                        return;
                    }

                    position += data;
                    readOffset += data;

                    if (position >= 2)
                    {
                        ushort size = (ushort)(((ushort)receiveBuffers[socket][0]) | ((ushort)receiveBuffers[socket][1] << 8));
                        targetLength = (int)size + 2;

                        if (targetLength > receiveBuffers[socket].Length)
                        {
                            // Message too long. Drop it and fix stuff by continuing the buffer
                            // TODO
                            HandleData(socket, 2, position, targetLength);
                        }
                        else
                        {
                            // Message is of an alright size.
                            if (position < size)
                            {
                                // We are not done reading yet. Continue
                                HandleData(socket, position, position, targetLength);
                            }
                            else
                            {
                                // We are done reading. Process the message now
                                Task.Run(() => HandleIncomingMessage(socket, 2, targetLength - 2).Wait()).ContinueWith((task) =>
                                {
                                    // Continue after
                                    HandleData(socket, 0, 0, 2);
                                });
                            }
                        }
                    }
                    else
                    {
                        // Only one byte, continue
                        HandleData(socket, readOffset, position, 2 - position);
                    }
                }, null);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        private static async Task HandleIncomingMessage(Socket socket, int offset, int size)
        {
            try
            {
                if (size <= 0)
                {
                    return;
                }

                using (MemoryStream stream = new MemoryStream(receiveBuffers[socket], offset, size))
                {
                    using (BinaryReader reader = new BinaryReader(stream, Encoding.UTF8, true))
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
                                        using (MemoryStream outStream = new MemoryStream())
                                        {
                                            using (BinaryWriter writer = new BinaryWriter(outStream, Encoding.UTF8, true))
                                            {
                                                writer.Write((byte)MessageType.RegisterAck);
                                                writer.Write(new Guid().ToString());
                                                writer.Write(false);
                                            }

                                            socket.BeginSend(outStream.GetBuffer(), 0, (int)outStream.Length, SocketFlags.None, (e) =>
                                            {
                                                socket.EndSend(e);
                                            }, null);
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
                                Address = ((IPEndPoint)socket.RemoteEndPoint).Address.MapToIPv6(),
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

                            using (MemoryStream outStream = new MemoryStream())
                            {
                                using (BinaryWriter writer = new BinaryWriter(outStream, Encoding.UTF8, true))
                                {
                                    writer.Write((byte)MessageType.RegisterAck);
                                    writer.Write(server.Id);
                                    writer.Write(true);
                                }

                                socket.BeginSend(outStream.GetBuffer(), 0, (int)outStream.Length, SocketFlags.None, (e) =>
                                {
                                    socket.EndSend(e);
                                }, null);
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
                                FilterDefinition<ServerModel> filter = Builders<ServerModel>.Filter.And(Builders<ServerModel>.Filter.Where(x => x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout)), QueryParser.CreateFilter(new List<JToken>() { parsedQuery }));

                                if (configuration.VerbosePrints)
                                {
                                    Console.WriteLine("[Query] Executing mongo query \"" + await mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").FindAsync(filter) + "\"");
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
                                serverModel = localModels.AsParallel().Where(x => x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout) && QueryParser.FilterLocalServers(new List<JToken>() { parsedQuery }, x)).ToList();
                            }

                            Console.WriteLine("[Query] Found " + (serverModel == null ? 0 : serverModel.Count) + " results. Total query time: " + (DateTime.Now - startTime).TotalMilliseconds + " ms");

                            using (MemoryStream outStream = new MemoryStream())
                            {
                                using (BinaryWriter writer = new BinaryWriter(outStream, Encoding.UTF8, true))
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

                                socket.BeginSend(outStream.GetBuffer(), 0, (int)outStream.Length, SocketFlags.None, (e) =>
                                {
                                    socket.EndSend(e);
                                }, null);
                            }
                        }
                        else if (messageType == (byte)MessageType.ServerAlive)
                        {
                            Console.WriteLine("[Alive] Started");
                            Guid guid = new Guid(reader.ReadString());

                            if (configuration.VerbosePrints)
                            {
                                Console.WriteLine("[Alive] Parsed from " + guid.ToString());
                            }
                            else
                            {
                                Console.WriteLine("[Alive] Parsed");
                            }

                            if (configuration.UseMongo)
                            {
                                // Find and validate address ownership
                                FilterDefinition<ServerModel> filter = Builders<ServerModel>.Filter.And(Builders<ServerModel>.Filter.Where(x => x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout)), Builders<ServerModel>.Filter.Eq(x => x.Address, ((IPEndPoint)socket.RemoteEndPoint).Address.MapToIPv6()), Builders<ServerModel>.Filter.Eq(x => x.Id, guid.ToString()));
                                // Create update
                                UpdateDefinition<ServerModel> update = Builders<ServerModel>.Update.Set(x => x.LastPingTime, DateTime.UtcNow);

                                // Execute
                                await mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").FindOneAndUpdateAsync(filter, update);
                            }
                            else
                            {
                                ServerModel model = localModels.Find(x => x.Address.Equals(((IPEndPoint)socket.RemoteEndPoint).Address.MapToIPv6()) && x.Id == guid.ToString() && x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout));

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

                            if (configuration.VerbosePrints)
                            {
                                Console.WriteLine("[Remove] Parsed from " + guid.ToString());
                            }
                            else
                            {
                                Console.WriteLine("[Remove] Parsed");
                            }

                            ServerModel model = null;

                            if (configuration.UseMongo)
                            {
                                // Find and validate address ownership
                                FilterDefinition<ServerModel> filter = Builders<ServerModel>.Filter.And(Builders<ServerModel>.Filter.Where(x => x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout)), Builders<ServerModel>.Filter.Eq(x => x.Address, ((IPEndPoint)socket.RemoteEndPoint).Address.MapToIPv6()), Builders<ServerModel>.Filter.Eq(x => x.Id, guid.ToString()));

                                // Execute
                                model = await mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").FindOneAndDeleteAsync(filter);
                            }
                            else
                            {
                                model = localModels.Find(x => x.Id == guid.ToString() && x.Address.Equals(((IPEndPoint)socket.RemoteEndPoint).Address.MapToIPv6()));

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
                                result = await (await mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").FindAsync(x => x.Id == guid.ToString() && x.Address == ((IPEndPoint)socket.RemoteEndPoint).Address.MapToIPv6() && x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout))).FirstOrDefaultAsync();
                            }
                            else
                            {
                                result = localModels.Find(x => x.Id == guid.ToString() && x.Address.Equals(((IPEndPoint)socket.RemoteEndPoint).Address.MapToIPv6()) && x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout));
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
                                    FilterDefinition<ServerModel> filter = Builders<ServerModel>.Filter.And(Builders<ServerModel>.Filter.Where(x => x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout)), Builders<ServerModel>.Filter.Eq(x => x.Address, ((IPEndPoint)socket.RemoteEndPoint).Address.MapToIPv6()), Builders<ServerModel>.Filter.Eq(x => x.Id, guid.ToString()));
                                    // Create update
                                    UpdateDefinition<ServerModel> update = Builders<ServerModel>.Update.Set(x => x.LastPingTime, DateTime.UtcNow).Set(x => x.ContractData, validatedLookupValues);

                                    // Insert model to DB
                                    await mongoClient.GetDatabase(configuration.MongoDatabase).GetCollection<ServerModel>("servers").FindOneAndUpdateAsync(filter, update);
                                }
                                else
                                {
                                    ServerModel model = localModels.Find(x => x.Address.Equals(((IPEndPoint)socket.RemoteEndPoint).Address.MapToIPv6()) && x.Id == guid.ToString() && x.LastPingTime >= DateTime.UtcNow.AddMilliseconds(-configuration.ServerTimeout));
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

                            using (MemoryStream outStream = new MemoryStream())
                            {
                                using (BinaryWriter writer = new BinaryWriter(outStream, Encoding.UTF8, true))
                                {
                                    writer.Write((byte)MessageType.ContractResponse);
                                    writer.Write(guid);
                                    writer.Write(ContractDefinition.IsCompatible(remoteContracts, contracts.Select(x => x.Value).ToArray()));
                                }

                                socket.BeginSend(outStream.GetBuffer(), 0, (int)outStream.Length, SocketFlags.None, (e) =>
                                {
                                    socket.EndSend(e);
                                }, null);
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }
}
