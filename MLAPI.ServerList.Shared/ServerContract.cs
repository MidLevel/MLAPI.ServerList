namespace MLAPI.ServerList.Shared
{
    public struct WeakContractDefinition
    {
        public string Name { get; set; }
        public ContractType Type { get; set; }
    }

    public struct ContractValue
    {
        public ContractDefinition Definition;
        public object Value;
    }
}
