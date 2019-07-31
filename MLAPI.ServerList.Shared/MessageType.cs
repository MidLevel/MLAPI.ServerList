namespace MLAPI.ServerList.Shared
{
    public enum MessageType
    {
        RegisterServer,
        RemoveServer,
        UpdateServer,
        ServerAlive,
        RegisterAck,
        Query,
        QueryResponse,
        ContractCheck,
        ContractResponse
    }
}
