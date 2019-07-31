using System;
using System.Collections.Generic;
using System.Net;

namespace MLAPI.ServerList.Client
{
    public class ServerModel
    {
        public Guid Id;
        public IPAddress Address { get; set; } = new IPAddress(0);
        public Dictionary<string, object> ContractData { get; set; } = new Dictionary<string, object>();
        public DateTime LastPingTime;
    }
}
