using System;
using System.Collections.Generic;
using System.Net;
using MongoDB.Bson.Serialization.Attributes;
using Newtonsoft.Json;

namespace MLAPI.ServerList.Server
{
    public class ServerModel
    {
        [BsonId]
        public string Id;
        [JsonIgnore]
        public IPAddress Address { get; set; } = new IPAddress(0);
        public Dictionary<string, object> ContractData { get; set; } = new Dictionary<string, object>();
        public DateTime LastPingTime;
    }
}
