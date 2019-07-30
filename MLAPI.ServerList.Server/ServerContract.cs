namespace MLAPI.ServerList.Server
{
    public struct ContractDefinition
    {
        public string Name { get; set; }
        public ContractType Type { get; set; }
        public bool Required { get; set; }
    }

    public struct ContractValue
    {
        public ContractDefinition Definition;
        public object Value;
    }

    public enum ContractType
    {
        Int8,
        Int16,
        Int32,
        Int64,
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        String,
        Buffer,
        Guid
    }
}
