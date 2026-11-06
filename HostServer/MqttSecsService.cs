using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;

namespace IndustrialCommunication
{
    // 結構化的 SECS/GEM MQTT 封包 Payload
    public class SecsGemMqttPacket
    {
        public string DeviceId { get; set; } = string.Empty;
        public int Stream { get; set; }        // 例: 6 (S6)
        public int Function { get; set; }      // 例: 11 (F11)
        public string Ceid { get; set; } = string.Empty; // CEID_1001
        public object Data { get; set; } = default!;
        public DateTime Timestamp { get; set; } = DateTime.Now;
    }

    // 獨立的 MQTT 通訊服務類別
    public class MqttSecsService
    {
        private readonly IMqttClient _mqttClient;
        private readonly MqttClientOptions _options;
        private readonly string _brokerIp;
        private readonly int _brokerPort;

        // 當收到 SECS 訊息時觸發的 C# Event
        public event Func<SecsGemMqttPacket, Task>? OnSecsMessageReceived;

        public MqttSecsService(string brokerIp, int brokerPort = 1883, string clientId = "")
        {
            _brokerIp = brokerIp;
            _brokerPort = brokerPort;

            var factory = new MqttFactory();
            _mqttClient = factory.CreateMqttClient();

            _options = new MqttClientOptionsBuilder()
                .WithTcpServer(_brokerIp, _brokerPort)
                .WithClientId(string.IsNullOrEmpty(clientId) ? $"Client_{Guid.NewGuid()}" : clientId)
                .WithCleanSession()
                .Build();

            // 註冊 MQTT 訊息接收事件
            _mqttClient.ApplicationMessageReceivedAsync += HandleApplicationMessageReceivedAsync;
        }

        // 啟動連線 (內建自動重連)
        public async Task StartAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    if (!_mqttClient.IsConnected)
                    {
                        Console.WriteLine($"🔄 [MQTT Client] 正在嘗試連線至 Broker ({_brokerIp}:{_brokerPort})...");
                        await _mqttClient.ConnectAsync(_options, ct);
                        Console.WriteLine("✅ [MQTT Client] 成功連線至 MQTT Broker！");
                    }
                    break;
                }
                catch (Exception)
                {
                    Console.WriteLine("❌ [MQTT Client] 連線失敗，5 秒後自動重試...");
                    await Task.Delay(5000, ct);
                }
            }
        }

        // 訂閱特定主題 (Topic)
        public async Task SubscribeAsync(string topic)
        {
            if (_mqttClient.IsConnected)
            {
                await _mqttClient.SubscribeAsync(topic);
                Console.WriteLine($"📡 [MQTT Subscribe] 已成功訂閱 Topic: {topic}");
            }
        }

        // 發送 SECS/GEM 封包 (Publish)
        public async Task PublishSecsEventAsync(string topic, SecsGemMqttPacket packet)
        {
            if (!_mqttClient.IsConnected) return;

            string json = JsonSerializer.Serialize(packet);
            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(Encoding.UTF8.GetBytes(json))
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            await _mqttClient.PublishAsync(message);
        }

        // 內部處理收到的 MQTT 訊息並轉抛給外部處理
        private async Task HandleApplicationMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs e)
        {
            string payload = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
            try
            {
                var packet = JsonSerializer.Deserialize<SecsGemMqttPacket>(payload);
                if (packet != null && OnSecsMessageReceived != null)
                {
                    await OnSecsMessageReceived.Invoke(packet);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️ [MQTT Driver Error] 封包解析失敗: {ex.Message}");
            }
        }
    }
}