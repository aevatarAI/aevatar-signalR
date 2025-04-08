using Aevatar.Core.Abstractions;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Collections.Concurrent;

namespace Aevatar.SignalR;

public class EventDeserializer
{
    // 使用线程安全的字典缓存事件类型
    private static readonly ConcurrentDictionary<string, Type> _eventTypeCache = new();
    private static readonly JsonSerializerSettings _serializerSettings = new()
    {
        TypeNameHandling = TypeNameHandling.None,
        NullValueHandling = NullValueHandling.Ignore,
        MaxDepth = 32 // 防止JSON爆炸攻击
    };
    private static bool _typesInitialized;
    private static readonly object _initLock = new();
    
    private readonly ILogger<EventDeserializer>? _logger;

    public EventDeserializer(ILogger<EventDeserializer>? logger = null)
    {
        _logger = logger;
        InitializeEventTypes();
    }

    private static void InitializeEventTypes()
    {
        // 确保类型只被初始化一次
        if (_typesInitialized) return;

        lock (_initLock)
        {
            if (_typesInitialized) return;
            
            try
            {
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
                            _eventTypeCache.TryAdd(type.FullName!, type);
                        }
                    }
                    catch (Exception)
                    {
                        // 忽略无法加载的程序集
                    }
                }
                
                _typesInitialized = true;
            }
            catch (Exception)
            {
                // 初始化失败时重置标志，允许重试
                _typesInitialized = false;
                throw;
            }
        }
    }

    public EventBase DeserializeEvent(string eventJson, string eventTypeName)
    {
        if (string.IsNullOrEmpty(eventJson))
            throw new ArgumentNullException(nameof(eventJson));
            
        if (string.IsNullOrEmpty(eventTypeName))
            throw new ArgumentNullException(nameof(eventTypeName));
            
        // 尝试从缓存获取类型
        if (!_eventTypeCache.TryGetValue(eventTypeName, out var eventType))
        {
            _logger?.LogWarning("Event type '{EventTypeName}' not found in cache", eventTypeName);
            
            // 重新尝试扫描类型（可能是运行时加载的新程序集）
            InitializeEventTypes();
            
            if (!_eventTypeCache.TryGetValue(eventTypeName, out eventType))
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