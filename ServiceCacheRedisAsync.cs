/* ------------------------------------------------------------------------- *
thZero.NetCore.Library.Services.Cache.Redis
Copyright (C) 2016-2021 thZero.com

<development [at] thzero [dot] com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 * ------------------------------------------------------------------------- */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Nito.AsyncEx;

using StackExchange.Redis;

using MsgPack.Serialization;

namespace thZero.Services
{
    public class ServiceCacheRedisAsync : ServiceCacheRedisBaseAsync<ServiceCacheRedisAsync>, IServiceCacheAsync
    {
        public ServiceCacheRedisAsync(ILogger<ServiceCacheRedisAsync> logger) : base(null, logger)
        {
        }
    }

    public class ServiceCacheRedisFactoryAsync : ServiceCacheRedisBaseAsync<ServiceCacheRedisFactoryAsync>, IServiceCacheAsync
    {
        private static readonly thZero.Services.IServiceLog log = thZero.Factory.Instance.RetrieveLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public ServiceCacheRedisFactoryAsync() : base(log, null)
        {
        }
    }

    public abstract class ServiceCacheRedisBaseAsync<TService> : ServiceBaseCacheAsync<TService>, IServiceCacheAsync
    {
        public ServiceCacheRedisBaseAsync(thZero.Services.IServiceLog log, ILogger<TService> logger) : base(log, logger)
        {
        }

        #region Protected Methods
        protected override async Task<bool> AddCore<T>(string key, T value, string region)
        {
            Enforce.AgainstNullOrEmpty(() => key);

            key = ValidateKey(key, region);

            //IServiceJson json = thZero.Utilities.Services.Json.Instance;
            //Enforce.AgainstNull(() => json);
            //string storeValueJ = json.Serialize(value);

            var serializer = SerializerInstance<T>();
            byte[] buffer = serializer.PackSingleObject(value);
            var storeValue = Convert.ToBase64String(buffer);

            IDatabase db = Instance.GetDatabase();

            await db.StringSetAsync(key, storeValue, new TimeSpan(8, 0, 0, 0), When.Always, CommandFlags.FireAndForget);
            await db.SetAddAsync(RegionKeySet, region, CommandFlags.FireAndForget);
            await db.SetAddAsync(region, key, CommandFlags.FireAndForget);

            var output = Convert.FromBase64String(storeValue);

            var result = serializer.UnpackSingleObject(output);

            return true;
        }

        protected override async Task<bool> ClearCore(string region)
        {
            if (string.IsNullOrEmpty(region))
                region = RegionKeyNone;
            region = region.ToLower();

            IDatabase db = Instance.GetDatabase();
            Enforce.AgainstNull(() => db);

            RedisValue[] values = db.SetMembers(region);
            foreach (var item in values)
            {
                if (item.IsNullOrEmpty)
                    continue;

                await db.KeyDeleteAsync(item.ToString(), CommandFlags.FireAndForget);
            }

            return true;
        }

        protected override async Task<bool> ClearAllCore()
        {
            IDatabase db = Instance.GetDatabase();
            Enforce.AgainstNull(() => db);

            IServer server;
            var endpoints = Instance.GetEndPoints(true);
            foreach (var endpoint in endpoints)
            {
                server = Instance.GetServer(endpoint);
                await server.FlushDatabaseAsync();
            }

            return true;
        }

        protected override async Task<bool> ContainsCore(string key, string region)
        {
            Enforce.AgainstNullOrEmpty(() => key);

            key = ValidateKey(key, region);

            IDatabase db = Instance.GetDatabase();
            Enforce.AgainstNull(() => db);

            return await db.KeyExistsAsync(key);
        }

        protected override async Task<T> GetCore<T>(string key, string region)
        {
            Enforce.AgainstNullOrEmpty(() => key);

            key = ValidateKey(key, region);

            //IProviderJson json = thZero.Utilities.Providers.Json.Instance;
            //Enforce.AgainstNull(() => json);

            IDatabase db = Instance.GetDatabase();
            Enforce.AgainstNull(() => db);

            if (await db.KeyExistsAsync(key))
            {
                string value = await db.StringGetAsync(key);

                //T result = json.Deserialize<T>(value);

                var serializer = SerializerInstance<T>();
                var output = Convert.FromBase64String(value);

                var result = serializer.UnpackSingleObject(output);
                return result;
            }

            return default;
        }

        protected override async Task<bool> InitializeCore(ServiceCacheConfig config)
        {
            Enforce.AgainstNull(() => config);

            if (_cache == null)
            {
                lock (_lockInstance)
                {
                    if (_cache == null)
                    {
                        var options = new ConfigurationOptions
                        {
                            EndPoints = { config.Endpoint },
                            Password = config.EndpointPassword,
                            AllowAdmin = true
                        };
                        _cache = ConnectionMultiplexer.Connect(options);
                    }
                }
            }

            return await Task.FromResult(true);
        }

        protected override async Task<bool> MaintainCacheCore()
        {
            return await Task.FromResult(true);
        }

        protected override async Task<bool> RemoveCore(string key, string region)
        {
            Enforce.AgainstNullOrEmpty(() => key);

            key = ValidateKey(key, region);

            IServiceJson json = thZero.Utilities.Services.Json.Instance;
            Enforce.AgainstNull(() => json);

            IDatabase db = Instance.GetDatabase();
            await db.KeyDeleteAsync(key, CommandFlags.FireAndForget);

            await db.SetRemoveAsync(RegionKeySet, region);
            await db.SetRemoveAsync(region, key);

            return true;
        }

        protected override async Task<long> SizeCore()
        {
            long? keys = null;

            IEnumerable<string> values;
            string value;
            string[] split;
            bool success;
            long temp = 0;
            IServer server;

            var endpoints = Instance.GetEndPoints(true);
            foreach (var endpoint in endpoints)
            {
                server = Instance.GetServer(endpoint);
                Enforce.AgainstNull(() => server);

                IGrouping<string, KeyValuePair<string, string>>[] info = await server.InfoAsync();
                foreach (IGrouping<string, KeyValuePair<string, string>> data in info)
                {
                    string key = data.Key;
                    if (key.EqualsIgnore("Keyspace"))
                    {
                        values = data.Select(l => l.Value);
                        value = values.FirstOrDefault();
                        split = value.Split(new[] { ',' });
                        if (split.Length > 0)
                            split = split[0].Split(new[] { '=' });
                        if (split.Length > 0)
                        {
                            value = split[1];
                            success = long.TryParse(value, out temp);
                            keys = (success ? (long?)temp : null);
                        }
                    }
                }

                break;
            }

            return keys ?? 0;
        }

        protected override async Task<Dictionary<string, long>> SizeRegionsCore()
        {
            //return await Task.Run(() =>
            //{
            Dictionary<string, long> list = new();

            string region = string.Empty;
            string[] values;

            var endpoints = Instance.GetEndPoints(true);
            foreach (var endpoint in endpoints)
            {
                var server = Instance.GetServer(endpoint);
                Enforce.AgainstNull(() => server);

                IEnumerable<RedisKey> keys = server.Keys();
                foreach (string cacheKey in keys)
                {
                    region = RegionKeyNone;

                    values = cacheKey.Split('-');
                    if (values.Length == 2)
                        region = values[0];

                    if (list.ContainsKey(region))
                        list[region] = list[region] + 1;
                    else
                        list.Add(region, 1);
                }

                break;
            }

            //return list;
            //});

            return await Task.FromResult(list);
        }

        protected async override Task<ServiceCacheStats> StatsCore()
        {
            ServiceCacheStats stats = new();

            IEnumerable<string> values;
            string value;
            string[] split;
            bool success;
            long temp = 0;
            //float tempF = 0;
            IServer server;

            var endpoints = Instance.GetEndPoints(true);
            foreach (var endpoint in endpoints)
            {
                server = Instance.GetServer(endpoint);
                Enforce.AgainstNull(() => server);

                IGrouping<string, KeyValuePair<string, string>>[] info = await server.InfoAsync();
                foreach (IGrouping<string, KeyValuePair<string, string>> data in info)
                {
                    string key = data.Key;
                    if (key.EqualsIgnore("Memory"))
                    {
                        values = data.Select(l => l.Value);
                        value = values.FirstOrDefault();
                        success = long.TryParse(value, out temp);
                        stats.Storage = (success ? (long?)temp : null);
                        //value = values.Skip(2).FirstOrDefault();
                        //success = long.TryParse(value, out temp);
                        //stats.UsedMemoryRss = (success ? (long?)temp : null);
                        value = values.Skip(3).FirstOrDefault();
                        success = long.TryParse(value, out temp);
                        stats.StorageMax = (success ? (long?)temp : null);
                    }
                    //else if (key.EqualsIgnore("Cpu"))
                    //{
                    //    values = data.Select(l => l.Value);
                    //    value = values.FirstOrDefault();
                    //    success = float.TryParse(value, out tempF);
                    //    stats.UsedCpu = (success ? (long?)tempF : null);
                    //}
                    else if (key.EqualsIgnore("Keyspace"))
                    {
                        values = data.Select(l => l.Value);
                        value = values.FirstOrDefault();
                        split = value.Split(new[] { ',' });
                        if (split.Length > 0)
                            split = split[0].Split(new[] { '=' });
                        if (split.Length > 0)
                        {
                            value = split[1];
                            success = long.TryParse(value, out temp);
                            stats.Keys = (success ? (long?)temp : null);
                            //if (success)
                            //   stats.Add("Keys", temp.ToString());
                        }
                    }
                }

                break;
            }

            return stats;
        }

        protected MessagePackSerializer<T> SerializerInstance<T>()
        {
            Type type = typeof(T);

            if (!_instanceSerializer.ContainsKey(type))
            {
                lock (_lockSerializer)
                {
                    if (!_instanceSerializer.ContainsKey(type))
                    {
                        MessagePackSerializer<T> serializer = MessagePackSerializer.Get<T>();
                        _instanceSerializer.Add(type, serializer);
                    }
                }
            }

            return (MessagePackSerializer<T>)_instanceSerializer[type];
        }
        #endregion

        #region Protected Properties
        protected override bool CacheLockEnabled { get { return false; } }
        protected ConnectionMultiplexer Instance { get { return _cache; } }
        protected override AsyncReaderWriterLock Lock { get { return _lock; } }
        #endregion

        #region Private Methods
        protected string ValidateKey(string key, string region = null)
        {
            if (string.IsNullOrEmpty(key))
                return key;

            if (!string.IsNullOrEmpty(region))
                key = string.Concat(region.ToLower(), "-", key);

            return key.ToLower();
        }
        #endregion

        #region Fields
        private ConnectionMultiplexer _cache;
        private static readonly Dictionary<Type, object> _instanceSerializer = new();
        private readonly AsyncReaderWriterLock _lock = new();
        private readonly object _lockInstance = new();
        private static readonly object _lockSerializer = new();
        #endregion

        #region Constants
        protected const string RegionKeyNone = "none";
        protected const string RegionKeySet = "regions";
        #endregion
    }
}
