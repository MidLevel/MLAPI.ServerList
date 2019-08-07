using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace MLAPI.ServerList.Shared
{
    public struct WeakContractDefinition
    {
        public string Name { get; set; }
        public ContractType Type { get; set; }
    }

    public struct ContractDefinition
    {
        public string Name { get; set; }
        [JsonConverter(typeof(StringEnumConverter))]
        public ContractType Type { get; set; }
        public bool Required { get; set; }

        // Checks if two contract definitions are compatible with each other
        public static bool IsCompatible(ContractDefinition[] v1, ContractDefinition[] v2)
        {
            // Contract conflict validation
            for (int i = 0; i < v1.Length; i++)
            {
                bool found = false;

                for (int j = 0; j < v2.Length; j++)
                {
                    if (v1[i].Name == v2[j].Name)
                    {
                        if (v1[i].Type != v2[j].Type)
                        {
                            return false;
                        }

                        found = true;
                    }
                }

                if (v1[i].Required && !found)
                {
                    // If required, fail if we dont find.
                    return false;
                }
            }


            // Contract conflict validation
            for (int i = 0; i < v2.Length; i++)
            {
                bool found = false;

                for (int j = 0; j < v1.Length; j++)
                {
                    if (v2[i].Name == v1[j].Name)
                    {
                        if (v2[i].Type != v1[j].Type)
                        {
                            return false;
                        }

                        found = true;
                    }
                }

                if (v2[i].Required && !found)
                {
                    // If required, fail if we dont find.
                    return false;
                }
            }

            return true;
        }

        public static bool IsCompatible(WeakContractDefinition[] v1, ContractDefinition[] v2)
        {
            // Contract conflict validation
            for (int i = 0; i < v1.Length; i++)
            {
                for (int j = 0; j < v2.Length; j++)
                {
                    if (v1[i].Name == v2[j].Name)
                    {
                        if (v1[i].Type != v2[j].Type)
                        {
                            return false;
                        }
                    }
                }
            }


            // Contract conflict validation
            for (int i = 0; i < v2.Length; i++)
            {
                bool found = false;

                for (int j = 0; j < v1.Length; j++)
                {
                    if (v2[i].Name == v1[j].Name)
                    {
                        if (v2[i].Type != v1[j].Type)
                        {
                            return false;
                        }

                        found = true;
                    }
                }

                if (v2[i].Required && !found)
                {
                    // If required, fail if we dont find.
                    return false;
                }
            }

            return true;
        }
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
