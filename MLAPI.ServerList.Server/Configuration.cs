using System;

namespace MLAPI.ServerList.Server
{
    public class Configuration
    {
        public ushort Port { get; set; } = 9423;
        public int BufferSize { get; set; } = 1024 * 8;
        public bool UseMongo { get; set; } = true;
        public string MongoConnection { get; set; } = "mongodb://127.0.0.1:27017";
        public string MongoDatabase { get; set; } = "listserver";
        public int ServerTimeout { get; set; } = 20_000;
        public int AckDelay { get; set; } = 5_000;
        public ContractDefinition[] ServerContract { get; set; } = new ContractDefinition[]
        {
            new ContractDefinition()
            {
                Name = "DEFAULT_ServerName",
                Required = false,
                Type = ContractType.String
            }
        };
    }
}
