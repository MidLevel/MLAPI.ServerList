using System;
using System.Collections.Generic;
using System.Linq;
using MLAPI.ServerList.Client;

namespace ClientExample
{
    class Program
    {
        static void Main(string[] args)
        {
            // Register 100 servers
            for (int i = 0; i < 1000; i++)
            {
                ServerConnection advertConnection = new ServerConnection();

                // Connect
                advertConnection.Connect("127.0.0.1", 9423);

                // Create server data
                Dictionary<string, object> data = new Dictionary<string, object>
                    {
                        { "Players", (int)i },
                        { "Name", "This is the name" }
                    };

                // Register server
                advertConnection.StartAdvertisment(data);
            }

            using (ServerConnection queryConnection = new ServerConnection())
            {
                // Connect
                queryConnection.Connect("127.0.0.1", 9423);

                // Send query
                List<ServerModel> models = queryConnection.SendQuery(@"
                {
                    ""$and"": [
                        {
                            ""Players"": {
                                ""$gte"": 20
                            }
                        },
                        {
                            ""Players"": {
                                ""$lte"": 50
                            }
                        },
                        {
                            ""Players"": {
                                ""$in"": [
                                    12,
                                    13,
                                    14,
                                    23,
                                    43,
                                    51
                                ]
                            }
                        }
                    ]
                }");

                Console.WriteLine(string.Format("| {0,5} | {1,5} | {2,5} |", "UUID", "Name", "Players"));
                Console.WriteLine(string.Join(Environment.NewLine, models.Select(x => string.Format("| {0,5} | {1,5} | {2,5} |", x.Id, x.ContractData["Name"], x.ContractData["Players"]))));
            }
        }
    }
}
