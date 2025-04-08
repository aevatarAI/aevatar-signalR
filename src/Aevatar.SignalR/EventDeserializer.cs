using Aevatar.Core.Abstractions;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Aevatar.SignalR;

public class EventDeserializer
{
    // 使用线程安全的字典缓存事件类型
    private static readonly Lazy<ConcurrentDictionary<string, Type>> _lazyEventTypeCache = new(
        () => InitializeEventTypes(),
        LazyThreadSafetyMode.ExecutionAndPublication);

    private static readonly JsonSerializerSettings _serializerSettings = new()
    {
        TypeNameHandling = TypeNameHandling.None,
        NullValueHandling = NullValueHandling.Ignore,
        MaxDepth = 32, // 防止JSON爆炸攻击
        DateFormatHandling = DateFormatHandling.IsoDateFormat,
        DateTimeZoneHandling = DateTimeZoneHandling.Utc
    };
    
    private readonly ILogger<EventDeserializer>? _logger;

    public EventDeserializer(ILogger<EventDeserializer>? logger = null)
    {
        _logger = logger;
    }

    private static ConcurrentDictionary<string, Type> InitializeEventTypes()
    {
        var cache = new ConcurrentDictionary<string, Type>();
        var assemblies = AppDomain.CurrentDomain.GetAssemblies();

        foreach (var assembly in assemblies)
        {
            try
            {
                var types = assembly.GetTypes()
                    .Where(t => t is { IsClass: true, IsAbstract: false } && 
                           typeof(EventBase).IsAssignableFrom(t));
                           
                foreach (var type in types)
                {
                    cache.TryAdd(type.FullName!, type);
                }
            }
            catch (Exception)
            {
                // 忽略无法加载的程序集
            }
        }
        
        return cache;
    }
    
    // 加载新程序集中的事件类型
    private void ScanNewAssemblies()
    {
        var cache = _lazyEventTypeCache.Value;
        var assemblies = AppDomain.CurrentDomain.GetAssemblies();
        
        foreach (var assembly in assemblies)
        {
            try
            {
                var types = assembly.GetTypes()
                    .Where(t => t is { IsClass: true, IsAbstract: false } && 
                           typeof(EventBase).IsAssignableFrom(t));
                           
                foreach (var type in types)
                {
                    cache.TryAdd(type.FullName!, type);
                }
            }
            catch (Exception)
            {
                // 忽略无法加载的程序集
            }
        }
    }

    public EventBase DeserializeEvent(string eventJson, string eventTypeName)
    {
        if (string.IsNullOrEmpty(eventJson))
            throw new ArgumentNullException(nameof(eventJson));
            
        if (string.IsNullOrEmpty(eventTypeName))
            throw new ArgumentNullException(nameof(eventTypeName));
            
        var cache = _lazyEventTypeCache.Value;
        
        // 尝试从缓存获取类型
        if (!cache.TryGetValue(eventTypeName, out var eventType))
        {
            _logger?.LogWarning("Event type '{EventTypeName}' not found in cache", eventTypeName);
            
            // 重新尝试扫描类型（可能是运行时加载的新程序集）
            ScanNewAssemblies();
            
            if (!cache.TryGetValue(eventTypeName, out eventType))
            {
                throw new InvalidOperationException($"Event type '{eventTypeName}' not found.");
            }
        }

        try
        {
            var result = JsonConvert.DeserializeObject(eventJson, eventType, _serializerSettings);
            if (result == null)
            {
                throw new JsonSerializationException($"Failed to deserialize event of type '{eventTypeName}'");
            }
            
            return (EventBase)result;
        }
        catch (JsonException ex)
        {
            _logger?.LogError(ex, "JSON deserialization error for type '{EventTypeName}'", eventTypeName);
            throw new InvalidOperationException($"Failed to deserialize event of type '{eventTypeName}': {ex.Message}", ex);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Unexpected error deserializing event of type '{EventTypeName}'", eventTypeName);
            throw;
        }
    }
}