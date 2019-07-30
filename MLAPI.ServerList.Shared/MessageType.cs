namespace MLAPI.ServerList.Shared
{
    public enum MessageType
    {
        RegisterServer,
        RemoveServer,
        UpdateServer,
        ServerAlive,
        Ack,
        Query,
        QueryResponse,
        Error
    }
}
