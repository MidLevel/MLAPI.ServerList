using System;
using System.Collections.Generic;
using System.Net;
using MongoDB.Bson.Serialization.Attributes;

namespace MLAPI.ServerList.Server
{
    public class ServerModel
    {
        [BsonId]
        public Guid Id;
        public IPAddress Address { get; set; } = new IPAddress(0);
        public Dictionary<string, object> ContractData { get; set; } = new Dictionary<string, object>();
        public DateTime LastPingTime;
    }
}
