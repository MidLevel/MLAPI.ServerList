using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using MLAPI.ServerList.Shared;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;

namespace MLAPI.ServerList.Server
{
    public static class QueryParser
    {
        public static bool FilterLocalServers(List<JToken> tokens, ServerModel serverModel)
        {
            string name = null;

            foreach (JToken child in tokens)
            {
                if (child.Type == JTokenType.Property)
                {
                    JProperty value = (JProperty)child;
                    name = value.Name;
                }

                switch (name)
                {
                    case "$not":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                List<JToken> children = child.Values().ToList();

                                return !FilterLocalServers(new List<JToken>() { children.First() }, serverModel);
                            }
                        }
                        break;
                    case "$and":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                List<JToken> children = child.Values().ToList();

                                return children.AsParallel().All(x => FilterLocalServers(new List<JToken>() { x }, serverModel));
                            }
                        }
                        break;
                    case "$or":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                List<JToken> children = child.Values().ToList();

                                return children.AsParallel().Any(x => FilterLocalServers(new List<JToken>() { x }, serverModel));
                            }
                        }
                        break;
                    case "$exists":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                string propertyName = ((JProperty)child.Parent.Parent).Name;

                                return serverModel.ContractData.ContainsKey(propertyName);
                            }
                        }
                        break;
                    case "$text":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                List<JToken> children = child.Values().ToList();

                                JToken searchToken = children.Where(x => x is JProperty property && property.Name == "$search").FirstOrDefault();
                                JToken caseSensitiveToken = children.Where(x => x is JProperty property && property.Name == "$caseSensitive").FirstOrDefault();

                                // TODO: Document this bad boy. This is NOT part of Mongo and is not supported when running with mongo.
                                JToken fieldSpecificToken = children.Where(x => x is JProperty property && property.Name == "$fieldSpecific").FirstOrDefault();

                                JToken languageToken = children.Where(x => x is JProperty property && property.Name == "$language").FirstOrDefault();
                                JToken diacriticSensitiveToken = children.Where(x => x is JProperty property && property.Name == "$diacriticSensitive").FirstOrDefault();

                                if (languageToken != null)
                                {
                                    Console.WriteLine("[Query] WARNING - $language is not yet supported on $text");
                                }

                                if (diacriticSensitiveToken != null)
                                {
                                    Console.WriteLine("[Query] WARNING - $diacriticSensitive is not yet supported on $text");
                                }

                                if (searchToken == null)
                                {
                                    Console.WriteLine("[Query] ERROR - $search property is required on $text");
                                    return false;
                                }

                                string searchString = searchToken.Values<string>().FirstOrDefault();

                                if (searchString == null)
                                {
                                    Console.WriteLine("[Query] WARNING - $search string on $text was empty");
                                    return false;
                                }

                                bool caseSensitive = caseSensitiveToken == null ? false : caseSensitiveToken.Values<bool>().FirstOrDefault();
                                bool fieldSpecific = fieldSpecificToken == null ? false : fieldSpecificToken.Values<bool>().FirstOrDefault();

                                string[] strings = searchString.Split(null);

                                string[] searchableFields = Program.configuration.ServerContract.Where(x => x.Type == ContractType.String && (!fieldSpecific || x.Name == ((JProperty)child.Parent.Parent).Name)).Select(x => x.Name).ToArray();

                                string[] values = serverModel.ContractData.Where(x => searchableFields.Contains(x.Key)).SelectMany(x => ((string)x.Value).Split(null)).ToArray();

                                return strings.Any(x => values.Any(y => x.Equals(y, caseSensitive ? StringComparison.InvariantCulture : StringComparison.InvariantCultureIgnoreCase)));
                            }
                        }
                        break;
                    case "$in":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                string propertyName = ((JProperty)child.Parent.Parent).Name;

                                JToken firstToken = child.Values().Values().FirstOrDefault();

                                if (firstToken != null && serverModel.ContractData.ContainsKey(propertyName))
                                {
                                    switch (firstToken.Type)
                                    {
                                        case JTokenType.Integer:
                                            return serverModel.ContractData[propertyName] is long && child.Values().Values<long>().Contains((long)serverModel.ContractData[propertyName]);
                                        case JTokenType.Float:
                                            return serverModel.ContractData[propertyName] is float && child.Values().Values<float>().Contains((float)serverModel.ContractData[propertyName]);
                                        case JTokenType.String:
                                            return serverModel.ContractData[propertyName] is string && child.Values().Values<string>().Contains((string)serverModel.ContractData[propertyName]);
                                        case JTokenType.Boolean:
                                            return serverModel.ContractData[propertyName] is bool && child.Values().Values<bool>().Contains((bool)serverModel.ContractData[propertyName]);
                                        case JTokenType.Date:
                                            return serverModel.ContractData[propertyName] is DateTime && child.Values().Values<DateTime>().Contains((DateTime)serverModel.ContractData[propertyName]);
                                        case JTokenType.Bytes:
                                            return serverModel.ContractData[propertyName] is byte[] && child.Values().Values<byte[]>().Contains((byte[])serverModel.ContractData[propertyName]);
                                        case JTokenType.Guid:
                                            return serverModel.ContractData[propertyName] is Guid && child.Values().Values<Guid>().Contains((Guid)serverModel.ContractData[propertyName]);
                                        case JTokenType.Uri:
                                            return serverModel.ContractData[propertyName] is Uri && child.Values().Values<Uri>().Contains((Uri)serverModel.ContractData[propertyName]);
                                        case JTokenType.TimeSpan:
                                            return serverModel.ContractData[propertyName] is TimeSpan && child.Values().Values<TimeSpan>().Contains((TimeSpan)serverModel.ContractData[propertyName]);
                                    }
                                }

                                // Fallback, no values provided
                                return false;
                            }
                        }
                        break;
                    case "$nin":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                string propertyName = ((JProperty)child.Parent.Parent).Name;

                                JToken firstToken = child.Values().Values().FirstOrDefault();

                                if (firstToken != null && serverModel.ContractData.ContainsKey(propertyName))
                                {
                                    switch (firstToken.Type)
                                    {
                                        case JTokenType.Integer:
                                            return serverModel.ContractData[propertyName] is long && !child.Values().Values<long>().Contains((long)serverModel.ContractData[propertyName]);
                                        case JTokenType.Float:
                                            return serverModel.ContractData[propertyName] is float && !child.Values().Values<float>().Contains((float)serverModel.ContractData[propertyName]);
                                        case JTokenType.String:
                                            return serverModel.ContractData[propertyName] is string && !child.Values().Values<string>().Contains((string)serverModel.ContractData[propertyName]);
                                        case JTokenType.Boolean:
                                            return serverModel.ContractData[propertyName] is bool && !child.Values().Values<bool>().Contains((bool)serverModel.ContractData[propertyName]);
                                        case JTokenType.Date:
                                            return serverModel.ContractData[propertyName] is DateTime && !child.Values().Values<DateTime>().Contains((DateTime)serverModel.ContractData[propertyName]);
                                        case JTokenType.Bytes:
                                            return serverModel.ContractData[propertyName] is byte[] && !child.Values().Values<byte[]>().Contains((byte[])serverModel.ContractData[propertyName]);
                                        case JTokenType.Guid:
                                            return serverModel.ContractData[propertyName] is Guid && !child.Values().Values<Guid>().Contains((Guid)serverModel.ContractData[propertyName]);
                                        case JTokenType.Uri:
                                            return serverModel.ContractData[propertyName] is Uri && !child.Values().Values<Uri>().Contains((Uri)serverModel.ContractData[propertyName]);
                                        case JTokenType.TimeSpan:
                                            return serverModel.ContractData[propertyName] is TimeSpan && !child.Values().Values<TimeSpan>().Contains((TimeSpan)serverModel.ContractData[propertyName]);
                                    }
                                }

                                // Fallback, no values provided
                                return true;
                            }
                        }
                        break;
                    case "$eq":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                string propertyName = ((JProperty)child.Parent.Parent).Name;

                                if (serverModel.ContractData.ContainsKey(propertyName))
                                {
                                    switch (child.Values().First().Type)
                                    {
                                        case JTokenType.Integer:
                                            return serverModel.ContractData[propertyName] is long && (long)serverModel.ContractData[propertyName] == child.Values().Values<long>().First();
                                        case JTokenType.Float:
                                            return serverModel.ContractData[propertyName] is float && Math.Abs((float)serverModel.ContractData[propertyName] - child.Values().Values<float>().First()) < 0.0001;
                                        case JTokenType.String:
                                            return serverModel.ContractData[propertyName] is string && (string)serverModel.ContractData[propertyName] == child.Values().Values<string>().First();
                                        case JTokenType.Boolean:
                                            return serverModel.ContractData[propertyName] is bool && (bool)serverModel.ContractData[propertyName] == child.Values().Values<bool>().First();
                                        case JTokenType.Date:
                                            return serverModel.ContractData[propertyName] is DateTime && (DateTime)serverModel.ContractData[propertyName] == child.Values().Values<DateTime>().First();
                                        case JTokenType.Bytes:
                                            return serverModel.ContractData[propertyName] is byte[] && ((byte[])serverModel.ContractData[propertyName]).SequenceEqual(child.Values().Values<byte[]>().First());
                                        case JTokenType.Guid:
                                            return serverModel.ContractData[propertyName] is Guid && (Guid)serverModel.ContractData[propertyName] == child.Values().Values<Guid>().First();
                                        case JTokenType.Uri:
                                            return serverModel.ContractData[propertyName] is Uri && (Uri)serverModel.ContractData[propertyName] == child.Values().Values<Uri>().First();
                                        case JTokenType.TimeSpan:
                                            return serverModel.ContractData[propertyName] is TimeSpan && (TimeSpan)serverModel.ContractData[propertyName] == child.Values().Values<TimeSpan>().First();
                                        case JTokenType.Null:
                                            return serverModel.ContractData[propertyName] == null;
                                        default:
                                            {
                                                Console.WriteLine("[Query] ERROR - Cannot operate $eq with value of type " + child.Values().First().Type);
                                                return false;
                                            }
                                    }
                                }
                                else
                                {
                                    // Value does not exist
                                    return false;
                                }
                            }
                        }
                        break;
                    case "$ne":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                string propertyName = ((JProperty)child.Parent.Parent).Name;

                                if (serverModel.ContractData.ContainsKey(propertyName))
                                {
                                    switch (child.Values().First().Type)
                                    {
                                        case JTokenType.Integer:
                                            return !(serverModel.ContractData[propertyName] is long) || (long)serverModel.ContractData[propertyName] != child.Values().Values<long>().First();
                                        case JTokenType.Float:
                                            return !(serverModel.ContractData[propertyName] is float) || Math.Abs((float)serverModel.ContractData[propertyName] - child.Values().Values<float>().First()) >= 0.0001;
                                        case JTokenType.String:
                                            return !(serverModel.ContractData[propertyName] is string) || (string)serverModel.ContractData[propertyName] != child.Values().Values<string>().First();
                                        case JTokenType.Boolean:
                                            return !(serverModel.ContractData[propertyName] is bool) || (bool)serverModel.ContractData[propertyName] != child.Values().Values<bool>().First();
                                        case JTokenType.Date:
                                            return !(serverModel.ContractData[propertyName] is DateTime) || (DateTime)serverModel.ContractData[propertyName] != child.Values().Values<DateTime>().First();
                                        case JTokenType.Bytes:
                                            return !(serverModel.ContractData[propertyName] is byte[]) || !((byte[])serverModel.ContractData[propertyName]).SequenceEqual(child.Values().Values<byte[]>().First());
                                        case JTokenType.Guid:
                                            return !(serverModel.ContractData[propertyName] is Guid) || (Guid)serverModel.ContractData[propertyName] != child.Values().Values<Guid>().First();
                                        case JTokenType.Uri:
                                            return !(serverModel.ContractData[propertyName] is Uri) || (Uri)serverModel.ContractData[propertyName] != child.Values().Values<Uri>().First();
                                        case JTokenType.TimeSpan:
                                            return !(serverModel.ContractData[propertyName] is TimeSpan) || (TimeSpan)serverModel.ContractData[propertyName] != child.Values().Values<TimeSpan>().First();
                                        case JTokenType.Null:
                                            return serverModel.ContractData[propertyName] != null;
                                        default:
                                            {
                                                Console.WriteLine("[Query] ERROR - Cannot operate $ne with value of type " + child.Values().First().Type);
                                                return false;
                                            }
                                    }
                                }
                                else
                                {
                                    // Value does not exist
                                    return false;
                                }
                            }
                        }
                        break;
                    case "$regex":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                JToken optionsToken = child.Values().Where(x => x is JProperty property && property.Name == "$options").FirstOrDefault();

                                if (optionsToken != null)
                                {
                                    Console.WriteLine("[Query] WARNING - $options is not yet supported on $regex");
                                }

                                string propertyName = ((JProperty)child.Parent.Parent).Name;

                                if (serverModel.ContractData.ContainsKey(propertyName) && serverModel.ContractData[propertyName] is string)
                                {
                                    switch (child.Values().First().Type)
                                    {
                                        case JTokenType.String:
                                            {
                                                return Regex.IsMatch((string)serverModel.ContractData[propertyName], child.Values().Values<string>().First());
                                            }
                                        default:
                                            {
                                                Console.WriteLine("[Query] ERROR - Cannot operate $regex with non string values");
                                                return false;
                                            }
                                    }
                                }
                                else
                                {
                                    // Value does not exist OR is the wrong type
                                    return false;
                                }
                            }
                        }
                        break;
                    case "$gt":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                string propertyName = ((JProperty)child.Parent.Parent).Name;

                                if (serverModel.ContractData.ContainsKey(propertyName))
                                {
                                    switch (child.Values().First().Type)
                                    {
                                        case JTokenType.Integer:
                                            return serverModel.ContractData[propertyName] is long && (long)serverModel.ContractData[propertyName] > child.Values().Values<long>().First();
                                        case JTokenType.Float:
                                            return serverModel.ContractData[propertyName] is float && (float)serverModel.ContractData[propertyName] > child.Values().Values<float>().First();
                                        case JTokenType.Date:
                                            return serverModel.ContractData[propertyName] is DateTime && (DateTime)serverModel.ContractData[propertyName] > child.Values().Values<DateTime>().First();
                                        case JTokenType.TimeSpan:
                                            return serverModel.ContractData[propertyName] is TimeSpan && (TimeSpan)serverModel.ContractData[propertyName] > child.Values().Values<TimeSpan>().First();
                                        default:
                                            {
                                                Console.WriteLine("[Query] ERROR - Cannot operate $gt with value of type " + child.Values().First().Type);
                                                return false;
                                            }
                                    }
                                }
                                else
                                {
                                    // Value does not exist
                                    return false;
                                }
                            }
                        }
                        break;
                    case "$gte":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                string propertyName = ((JProperty)child.Parent.Parent).Name;

                                if (serverModel.ContractData.ContainsKey(propertyName))
                                {
                                    switch (child.Values().First().Type)
                                    {
                                        case JTokenType.Integer:
                                            return serverModel.ContractData[propertyName] is long && (long)serverModel.ContractData[propertyName] >= child.Values().Values<long>().First();
                                        case JTokenType.Float:
                                            return serverModel.ContractData[propertyName] is float && (float)serverModel.ContractData[propertyName] >= child.Values().Values<float>().First();
                                        case JTokenType.Date:
                                            return serverModel.ContractData[propertyName] is DateTime && (DateTime)serverModel.ContractData[propertyName] >= child.Values().Values<DateTime>().First();
                                        case JTokenType.TimeSpan:
                                            return serverModel.ContractData[propertyName] is TimeSpan && (TimeSpan)serverModel.ContractData[propertyName] >= child.Values().Values<TimeSpan>().First();
                                        default:
                                            {
                                                Console.WriteLine("[Query] ERROR - Cannot operate $gte with value of type " + child.Values().First().Type);
                                                return false;
                                            }
                                    }
                                }
                                else
                                {
                                    // Value does not exist
                                    return false;
                                }
                            }
                        }
                        break;
                    case "$lt":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                string propertyName = ((JProperty)child.Parent.Parent).Name;

                                if (serverModel.ContractData.ContainsKey(propertyName))
                                {
                                    switch (child.Values().First().Type)
                                    {
                                        case JTokenType.Integer:
                                            return serverModel.ContractData[propertyName] is long && (long)serverModel.ContractData[propertyName] < child.Values().Values<long>().First();
                                        case JTokenType.Float:
                                            return serverModel.ContractData[propertyName] is float && (float)serverModel.ContractData[propertyName] < child.Values().Values<float>().First();
                                        case JTokenType.Date:
                                            return serverModel.ContractData[propertyName] is DateTime && (DateTime)serverModel.ContractData[propertyName] < child.Values().Values<DateTime>().First();
                                        case JTokenType.TimeSpan:
                                            return serverModel.ContractData[propertyName] is TimeSpan && (TimeSpan)serverModel.ContractData[propertyName] < child.Values().Values<TimeSpan>().First();
                                        default:
                                            {
                                                Console.WriteLine("[Query] ERROR - Cannot operate $lt with value of type " + child.Values().First().Type);
                                                return false;
                                            }
                                    }
                                }
                                else
                                {
                                    // Value does not exist
                                    return false;
                                }
                            }
                        }
                        break;
                    case "$lte":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                string propertyName = ((JProperty)child.Parent.Parent).Name;

                                if (serverModel.ContractData.ContainsKey(propertyName))
                                {
                                    switch (child.Values().First().Type)
                                    {
                                        case JTokenType.Integer:
                                            return serverModel.ContractData[propertyName] is long && (long)serverModel.ContractData[propertyName] <= child.Values().Values<long>().First();
                                        case JTokenType.Float:
                                            return serverModel.ContractData[propertyName] is float && (float)serverModel.ContractData[propertyName] <= child.Values().Values<float>().First();
                                        case JTokenType.Date:
                                            return serverModel.ContractData[propertyName] is DateTime && (DateTime)serverModel.ContractData[propertyName] <= child.Values().Values<DateTime>().First();
                                        case JTokenType.TimeSpan:
                                            return serverModel.ContractData[propertyName] is TimeSpan && (TimeSpan)serverModel.ContractData[propertyName] <= child.Values().Values<TimeSpan>().First();
                                        default:
                                            {
                                                Console.WriteLine("[Query] ERROR - Cannot operate $lte with value of type " + child.Values().First().Type);
                                                return false;
                                            }
                                    }
                                }
                                else
                                {
                                    // Value does not exist
                                    return false;
                                }
                            }
                        }
                        break;
                    default:
                        {
                            if (child.Type == JTokenType.Property || child.Type == JTokenType.Object)
                            {
                                return FilterLocalServers(child.Children().ToList(), serverModel);
                            }
                        }
                        break;
                }
            }

            return true;
        }

        public static FilterDefinition<ServerModel> CreateFilter(List<JToken> tokens)
        {
            string name = null;

            foreach (var child in tokens)
            {
                if (child.Type == JTokenType.Property)
                {
                    JProperty value = (JProperty)child;
                    name = value.Name;
                }

                switch (name)
                {
                    case "$not":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                return Builders<ServerModel>.Filter.Not(CreateFilter(new List<JToken>() { child.Values().First() }));
                            }
                        }
                        break;
                    case "$and":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                List<JToken> children = child.Values().ToList();
                                List<FilterDefinition<ServerModel>> childFilters = new List<FilterDefinition<ServerModel>>();

                                for (int i = 0; i < children.Count; i++)
                                {
                                    childFilters.Add(CreateFilter(new List<JToken>() { children[i] }));
                                }

                                return Builders<ServerModel>.Filter.And(childFilters);
                            }
                        }
                        break;
                    case "$or":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                List<JToken> children = child.Values().ToList();
                                List<FilterDefinition<ServerModel>> childFilters = new List<FilterDefinition<ServerModel>>();

                                for (int i = 0; i < children.Count; i++)
                                {
                                    childFilters.Add(CreateFilter(new List<JToken>() { children[i] }));
                                }

                                return Builders<ServerModel>.Filter.Or(childFilters);
                            }
                        }
                        break;
                    case "$exists":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                return Builders<ServerModel>.Filter.Exists("ContractData." + ((JProperty)child.Parent.Parent).Name);
                            }
                        }
                        break;
                    case "$text":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                List<JToken> children = child.Values().ToList();

                                JToken searchToken = children.Where(x => x is JProperty property && property.Name == "$search").FirstOrDefault();
                                JToken caseSensitiveToken = children.Where(x => x is JProperty property && property.Name == "$caseSensitive").FirstOrDefault();

                                // TODO: Document this bad boy. This is NOT part of Mongo and is not supported when running with mongo.
                                JToken fieldSpecificToken = children.Where(x => x is JProperty property && property.Name == "$fieldSpecific").FirstOrDefault();

                                JToken languageToken = children.Where(x => x is JProperty property && property.Name == "$language").FirstOrDefault();
                                JToken diacriticSensitiveToken = children.Where(x => x is JProperty property && property.Name == "$diacriticSensitive").FirstOrDefault();

                                if (languageToken != null)
                                {
                                    Console.WriteLine("[Query] WARNING - $language is not yet supported on $text");
                                }

                                if (diacriticSensitiveToken != null)
                                {
                                    Console.WriteLine("[Query] WARNING - $diacriticSensitive is not yet supported on $text");
                                }

                                if (fieldSpecificToken != null)
                                {
                                    Console.WriteLine("[Query] WARNING - $fieldSpecific is not supported when running with mongo");
                                }

                                if (searchToken == null)
                                {
                                    Console.WriteLine("[Query] ERROR - $search property is required on $text");
                                    return Builders<ServerModel>.Filter.Where(x => false);
                                }

                                string searchString = searchToken.Values<string>().FirstOrDefault();

                                if (searchString == null)
                                {
                                    Console.WriteLine("[Query] WARNING - $search string on $text was empty");
                                    return Builders<ServerModel>.Filter.Where(x => false);
                                }

                                bool caseSensitive = caseSensitiveToken.Values<bool>().FirstOrDefault();

                                return Builders<ServerModel>.Filter.Text(searchString, new TextSearchOptions()
                                {
                                    CaseSensitive = caseSensitive
                                });
                            }
                        }
                        break;
                    case "$in":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                JToken firstToken = child.Values().Values().FirstOrDefault();

                                if (firstToken != null)
                                {
                                    switch (firstToken.Type)
                                    {
                                        case JTokenType.Integer:
                                            return Builders<ServerModel>.Filter.In("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<long>());
                                        case JTokenType.Float:
                                            return Builders<ServerModel>.Filter.In("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<float>());
                                        case JTokenType.String:
                                            return Builders<ServerModel>.Filter.In("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<string>());
                                        case JTokenType.Boolean:
                                            return Builders<ServerModel>.Filter.In("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>());
                                        case JTokenType.Date:
                                            return Builders<ServerModel>.Filter.In("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>());
                                        case JTokenType.Bytes:
                                            return Builders<ServerModel>.Filter.In("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>());
                                        case JTokenType.Guid:
                                            return Builders<ServerModel>.Filter.In("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>());
                                        case JTokenType.Uri:
                                            return Builders<ServerModel>.Filter.In("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>());
                                        case JTokenType.TimeSpan:
                                            return Builders<ServerModel>.Filter.In("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>());
                                    }
                                }

                                // Fallback, no values provided
                                return Builders<ServerModel>.Filter.Where(x => false);
                            }
                        }
                        break;
                    case "$nin":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                JToken firstToken = child.Values().Values().FirstOrDefault();

                                if (firstToken != null)
                                {
                                    switch (firstToken.Type)
                                    {
                                        case JTokenType.Integer:
                                            return Builders<ServerModel>.Filter.Nin("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<long>());
                                        case JTokenType.Float:
                                            return Builders<ServerModel>.Filter.Nin("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<float>());
                                        case JTokenType.String:
                                            return Builders<ServerModel>.Filter.Nin("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<string>());
                                        case JTokenType.Boolean:
                                            return Builders<ServerModel>.Filter.Nin("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>());
                                        case JTokenType.Date:
                                            return Builders<ServerModel>.Filter.Nin("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>());
                                        case JTokenType.Bytes:
                                            return Builders<ServerModel>.Filter.Nin("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>());
                                        case JTokenType.Guid:
                                            return Builders<ServerModel>.Filter.Nin("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>());
                                        case JTokenType.Uri:
                                            return Builders<ServerModel>.Filter.Nin("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>());
                                        case JTokenType.TimeSpan:
                                            return Builders<ServerModel>.Filter.Nin("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>());
                                    }
                                }

                                // Fallback, no values provided. Thus its NOT inside
                                return Builders<ServerModel>.Filter.Where(x => true);
                            }
                        }
                        break;
                    case "$eq":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                switch (child.Values().First().Type)
                                {
                                    case JTokenType.Integer:
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<long>().First());
                                    case JTokenType.Float:
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<float>().First());
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    case JTokenType.Boolean:
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>().First());
                                    case JTokenType.Date:
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>().First());
                                    case JTokenType.Bytes:
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>().First());
                                    case JTokenType.Guid:
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>().First());
                                    case JTokenType.Uri:
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>().First());
                                    case JTokenType.TimeSpan:
                                        return Builders<ServerModel>.Filter.Eq("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>().First());
                                    default:
                                        {
                                            Console.WriteLine("[Query] ERROR - Cannot operate $eq with value of type " + child.Values().First().Type);
                                            return Builders<ServerModel>.Filter.Where(x => false);
                                        }
                                }
                            }
                        }
                        break;
                    case "$ne":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                switch (child.Values().First().Type)
                                {
                                    case JTokenType.Integer:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<long>().First());
                                    case JTokenType.Float:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<float>().First());
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    case JTokenType.Boolean:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>().First());
                                    case JTokenType.Date:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>().First());
                                    case JTokenType.Bytes:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>().First());
                                    case JTokenType.Guid:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>().First());
                                    case JTokenType.Uri:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>().First());
                                    case JTokenType.TimeSpan:
                                        return Builders<ServerModel>.Filter.Ne("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>().First());
                                    default:
                                        {
                                            Console.WriteLine("[Query] ERROR - Cannot operate $eq with value of type " + child.Values().First().Type);
                                            return Builders<ServerModel>.Filter.Where(x => false);
                                        }
                                }
                            }
                        }
                        break;
                    case "$regex":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                JToken optionsToken = child.Values().Where(x => x is JProperty property && property.Name == "$options").FirstOrDefault();

                                if (optionsToken != null)
                                {
                                    Console.WriteLine("[Query] WARNING - $options is not yet supported on $regex");
                                }

                                switch (child.Values().First().Type)
                                {
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Regex("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    default:
                                        {
                                            Console.WriteLine("[Query] ERROR - Cannot operate $regex with non string values");
                                            return Builders<ServerModel>.Filter.Where(x => false);
                                        }
                                }
                            }
                        }
                        break;
                    case "$gt":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                switch (child.Values().First().Type)
                                {
                                    case JTokenType.Integer:
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<long>().First());
                                    case JTokenType.Float:
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<float>().First());
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    case JTokenType.Boolean:
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>().First());
                                    case JTokenType.Date:
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>().First());
                                    case JTokenType.Bytes:
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>().First());
                                    case JTokenType.Guid:
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>().First());
                                    case JTokenType.Uri:
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>().First());
                                    case JTokenType.TimeSpan:
                                        return Builders<ServerModel>.Filter.Gt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>().First());
                                    default:
                                        {
                                            Console.WriteLine("[Query] ERROR - Cannot operate $gt with value of type " + child.Values().First().Type);
                                            return Builders<ServerModel>.Filter.Where(x => false);
                                        }
                                }
                            }
                        }
                        break;
                    case "$gte":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                switch (child.Values().First().Type)
                                {
                                    case JTokenType.Integer:
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<long>().First());
                                    case JTokenType.Float:
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<float>().First());
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    case JTokenType.Boolean:
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>().First());
                                    case JTokenType.Date:
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>().First());
                                    case JTokenType.Bytes:
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>().First());
                                    case JTokenType.Guid:
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>().First());
                                    case JTokenType.Uri:
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>().First());
                                    case JTokenType.TimeSpan:
                                        return Builders<ServerModel>.Filter.Gte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>().First());
                                    default:
                                        {
                                            Console.WriteLine("[Query] ERROR - Cannot operate $gte with value of type " + child.Values().First().Type);
                                            return Builders<ServerModel>.Filter.Where(x => false);
                                        }
                                }
                            }
                        }
                        break;
                    case "$lt":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                switch (child.Values().First().Type)
                                {
                                    case JTokenType.Integer:
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<long>().First());
                                    case JTokenType.Float:
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<float>().First());
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    case JTokenType.Boolean:
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>().First());
                                    case JTokenType.Date:
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>().First());
                                    case JTokenType.Bytes:
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>().First());
                                    case JTokenType.Guid:
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>().First());
                                    case JTokenType.Uri:
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>().First());
                                    case JTokenType.TimeSpan:
                                        return Builders<ServerModel>.Filter.Lt("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>().First());
                                    default:
                                        {
                                            Console.WriteLine("[Query] ERROR - Cannot operate $lt with value of type " + child.Values().First().Type);
                                            return Builders<ServerModel>.Filter.Where(x => false);
                                        }
                                }
                            }
                        }
                        break;
                    case "$lte":
                        {
                            if (child.Type == JTokenType.Property)
                            {
                                switch (child.Values().First().Type)
                                {
                                    case JTokenType.Integer:
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<long>().First());
                                    case JTokenType.Float:
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<float>().First());
                                    case JTokenType.String:
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<string>().First());
                                    case JTokenType.Boolean:
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<bool>().First());
                                    case JTokenType.Date:
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<DateTime>().First());
                                    case JTokenType.Bytes:
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<byte[]>().First());
                                    case JTokenType.Guid:
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Guid>().First());
                                    case JTokenType.Uri:
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<Uri>().First());
                                    case JTokenType.TimeSpan:
                                        return Builders<ServerModel>.Filter.Lte("ContractData." + ((JProperty)child.Parent.Parent).Name, child.Values().Values<TimeSpan>().First());
                                    default:
                                        {
                                            Console.WriteLine("[Query] ERROR - Cannot operate $lte with value of type " + child.Values().First().Type);
                                            return Builders<ServerModel>.Filter.Where(x => false);
                                        }
                                }
                            }
                        }
                        break;
                    default:
                        {
                            if (child.Type == JTokenType.Property || child.Type == JTokenType.Object)
                            {
                                return CreateFilter(child.Children().ToList());
                            }
                        }
                        break;
                }
            }

            return Builders<ServerModel>.Filter.Empty;
        }
    }
}
