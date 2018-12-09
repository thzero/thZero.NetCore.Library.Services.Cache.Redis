/* ------------------------------------------------------------------------- *
thZero.NetCore.Library.Services.Cache.Redis
Copyright (C) 2016-2018 thZero.com

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
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using StackExchange.Redis;

using Force.DeepCloner;

namespace thZero.Services
{
    public sealed class ServiceCacheRedisExecuteAsync : ServiceCacheRedisExecuteBaseAsync<ServiceCacheRedisExecuteAsync>, IServiceCacheExecute
    {
        public ServiceCacheRedisExecuteAsync(ILogger<ServiceCacheRedisExecuteAsync> logger) : base(null, logger)
        {
        }
    }

    public sealed class ServiceCacheRedisExecuteFactoryAsync : ServiceCacheRedisExecuteBaseAsync<ServiceCacheRedisExecuteFactoryAsync>, IServiceCacheExecute
    {
        private static readonly thZero.Services.IServiceLog log = thZero.Factory.Instance.RetrieveLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public ServiceCacheRedisExecuteFactoryAsync() : base(log, null)
        {
        }
    }

    public abstract class ServiceCacheRedisExecuteBaseAsync<TService> : ServiceCacheRedisBaseAsync<TService>
    {
        public ServiceCacheRedisExecuteBaseAsync(thZero.Services.IServiceLog log, ILogger<TService> logger) : base(log, logger)
        {
        }

        #region Public Methods
        public async Task<bool> Add<T>(IServiceCacheExecutableItemWithKey<T> executable, T value)
            where T : IServiceCacheExecuteResponse
        {
            Enforce.AgainstNull(() => executable);

            return await Add(executable.CacheKey, value, null, false);
        }

        public async Task<bool> Add<T>(IServiceCacheExecutableItemWithKey<T> executable, T value, string region)
            where T : IServiceCacheExecuteResponse
        {
            Enforce.AgainstNull(() => executable);

            return await Add(executable.CacheKey, value, region, false);
        }

        public async Task<bool> Add<T>(IServiceCacheExecutableItemWithKey<T> executable, T value, string region, bool forceCache)
            where T : IServiceCacheExecuteResponse
        {
            Enforce.AgainstNull(() => executable);

            return await Add(executable.CacheKey, value, region, forceCache);
        }

        public async Task<T> Check<T>(IServiceCacheExecutableItemWithKey<T> executable, string region)
            where T : IServiceCacheExecuteResponse
        {
            return await Check<T>(executable, region, true, false);
        }

        public async Task<T> Check<T>(IServiceCacheExecutableItemWithKey<T> executable, string region, bool forceCache)
            where T : IServiceCacheExecuteResponse
        {
            return await Check<T>(executable, region, true, forceCache);
        }

        public async Task<T> Check<T>(IServiceCacheExecutableItemWithKey<T> executable, string region, bool execute, bool forceCache)
            where T : IServiceCacheExecuteResponse
        {
            return await Check<T>(executable, null, region, execute, forceCache);
        }

        public async Task<T> Check<T>(IServiceCacheExecutableItemWithKey<T> executable, IServiceCacheExecute secondary, string region)
            where T : IServiceCacheExecuteResponse
        {
            return await Check<T>(executable, secondary, region, true, false);
        }

        public async Task<T> Check<T>(IServiceCacheExecutableItemWithKey<T> executable, IServiceCacheExecute secondary, string region, bool forceCache)
            where T : IServiceCacheExecuteResponse
        {
            return await Check<T>(executable, secondary, region, true, forceCache);
        }

        public async Task<T> Check<T>(IServiceCacheExecutableItemWithKey<T> executable, IServiceCacheExecute secondary, string region, bool execute, bool forceCache)
            where T : IServiceCacheExecuteResponse
        {
            const string Declaration = "Check";

            Enforce.AgainstNull(() => executable);

            try
            {
                List<Exception> exceptions = new List<Exception>();

                if (!UseCache(forceCache))
                {
                    var responseNoCache = await executable.ExecuteAsync();
                    return responseNoCache;
                }

                string cacheKey = executable.CacheKey;

                // If there was a secondary cache provided, check to see it was in the secondary cache first.
                if (secondary != null)
                {
                    try
                    {
                        T temp1 = await secondary.Get<T>(cacheKey, region);
                        if (temp1 != null)
                        {
                            T temp1a = temp1.DeepClone<T>();

                            temp1a.ClearDurations();
                            temp1a.WasCachedSecondary = true;
                            temp1a.CacheEnabled = CacheEnabled;

                            return temp1a;
                        }
                    }
                    catch (Exception ex)
                    {
                        Log?.Error(Declaration, ex);
                        Logger?.LogError(Declaration, ex);
                        exceptions.Add(ex);
                    }
                }

                if (await ContainsCore(cacheKey, region))
                {
                    try
                    {
                        T temp2 = await GetCore<T>(cacheKey, region);
                        if (temp2 != null)
                        {
                            T temp2a = temp2.DeepClone<T>();

                            temp2a.ClearDurations();
                            temp2a.WasCached = true;
                            temp2a.CacheEnabled = CacheEnabled;

                            if (secondary != null)
                            {
                                IDisposable lockResult2 = null;
                                try
                                {
                                    lockResult2 = await Lock.WriterLockAsync();

                                    await secondary.Add(executable, temp2, region);
                                }
                                catch (Exception ex)
                                {
                                    Log?.Error(Declaration, ex);
                                    Logger?.LogError(Declaration, ex);
                                    exceptions.Add(ex);
                                }
                                finally
                                {
                                    if (lockResult2 != null)
                                        lockResult2.Dispose();
                                }
                            }

                            return temp2a;
                        }
                    }
                    catch (Exception ex)
                    {
                        Log?.Error(Declaration, ex);
                        Logger?.LogError(Declaration, ex);
                        exceptions.Add(ex);
                    }
                }

                if (!execute)
                    return default(T);

                bool resultPrimary = true;
                bool resultSecondary = true;

                T response = await executable.ExecuteAsync();
                if ((response != null) && response.Cacheable)
                {
                    IEnumerable<Guid> ids = response.Ids;

                    IDisposable lockResult2 = null;
                    try
                    {
                        if (CacheLockEnabled)
                            lockResult2 = await Lock.WriterLockAsync();

                        // Let's go ahead and see if while we went and go stuff from the
                        // db, someone did that already, if so use it...
                        // otherwise with multiple servers, we'd just keep adding stuff
                        // to the cache all the time..
                        if (await ContainsCore(cacheKey, region))
                        {
                            try
                            {
                                T temp2 = await GetCore<T>(cacheKey, region);
                                if (temp2 != null)
                                {
                                    T temp2a = temp2.DeepClone<T>();

                                    temp2a.ClearDurations();
                                    temp2a.WasCached = true;
                                    temp2a.CacheEnabled = CacheEnabled;

                                    return temp2a;

                                }
                            }
                            catch (Exception ex)
                            {
                                Log?.Error(Declaration, ex);
                                Logger?.LogError(Declaration, ex);
                                exceptions.Add(ex);
                            }
                        }

                        try
                        {
                            resultPrimary = await AddCore<T>(cacheKey, response, region);
                            //if (resultPrimary)
                            //    await UpdateIndex(cacheKey, ids); // TODO: Cache
                        }
                        catch (Exception ex)
                        {
                            Log?.Error(Declaration, ex);
                            Logger?.LogError(Declaration, ex);
                            exceptions.Add(ex);
                        }

                        try
                        {
                            // If there was a secondary cache provided, add it to the secondary cache.
                            if (secondary != null)
                                resultSecondary = await secondary.Add(executable, response, region);
                        }
                        catch (Exception ex)
                        {
                            Log?.Error(Declaration, ex);
                            Logger?.LogError(Declaration, ex);
                            exceptions.Add(ex);
                        }
                    }
                    catch (Exception ex)
                    {
                        Log?.Error(Declaration, ex);
                        Logger?.LogError(Declaration, ex);
                        exceptions.Add(ex);
                    }
                    finally
                    {
                        if (lockResult2 != null)
                            lockResult2.Dispose();
                    }
                }

                bool result = resultPrimary && resultSecondary;

                response.CacheEnabled = CacheEnabled;
                return response;

                //bool resultPrimary = true;
                //bool resultSecondary = true;
                //var response = await executable.ExecuteAsync();
                //if ((response != null) && response.Cacheable)
                //{
                //    IDisposable lockResult2 = null;
                //    try
                //    {
                //        if (CacheLockEnabled)
                //            lockResult2 = await lockResult.UpgradeAsync();

                //        resultPrimary = await AddCore<T>(executable.CacheKey, response, region);

                //        if (secondary != null)
                //            resultSecondary = await secondary.Add(executable, response, region);
                //    }
                //    finally
                //    {
                //        if (lockResult2 != null)
                //            lockResult2.Dispose();
                //    }
                //}

                //bool result = resultPrimary && resultSecondary;

                //response.CacheEnabled = CacheEnabled;
                //return response;
            }
            catch (Exception ex)
            {
                Log?.Error(Declaration, ex);
                Logger?.LogError(Declaration, ex);
                throw;
            }
        }
        #endregion

        #region Private Methods
        private async Task<bool> UpdateIndex(string cacheKey, List<Guid> ids)
        {
            Enforce.AgainstNullOrEmpty(() => cacheKey);

            if (ids == null)
                return true;

            IDatabase db = Instance.GetDatabase();

            foreach (Guid id in ids)
                await db.SetAddAsync(id.ToString(), cacheKey);

            return true;
        }
        #endregion
    }
}
