using MLAPI.ServerList.Shared;

namespace MLAPI.ServerList.Server
{
    public class Configuration
    {
        public ushort Port { get; set; } = 9423;
        public string ListenAddress { get; set; } = "0.0.0.0";
        public bool VerbosePrints = true;
        public bool UseMongo { get; set; } = false;
        public string MongoConnection { get; set; } = "mongodb://127.0.0.1:27017";
        public int CollectionExpiryDelay { get; set; } = 20 * 60 * 1000;
        public string MongoDatabase { get; set; } = "listserver";
        public int ServerTimeout { get; set; } = 20_000;

        public ContractDefinition[] ServerContract { get; set; } = new ContractDefinition[]
        {
            new ContractDefinition()
            {
                Name = "Name",
                Required = false,
                Type = ContractType.String
            },
            new ContractDefinition()
            {
                Name = "Players",
                Required = true,
                Type = ContractType.Int32
            }
        };
    }
}
