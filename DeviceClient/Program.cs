using System;
using System.Threading;
using System.Threading.Tasks;
using IndustrialCommunication;

namespace DeviceClient
{
    class Program
    {
        private static readonly string MqttBrokerIp = Environment.GetEnvironmentVariable("MQTT_HOST") ?? "172.21.0.10";
        private const int MqttBrokerPort = 1883;

        static async Task Main(string[] args)
        {
            Console.WriteLine("==================================================");
            Console.WriteLine($"🤖 [DEVICE CLIENT] SECS/GEM over MQTT Simulator");
            Console.WriteLine($"   Target Broker: {MqttBrokerIp}:{MqttBrokerPort}");
            Console.WriteLine("==================================================");

            using var cts = new CancellationTokenSource();

            // 捕捉 Docker 容器停止訊號 (SIGTERM / SIGINT) 實作優雅關機
            Console.CancelKeyPress += (s, e) =>
            {
                Console.WriteLine("🛑 收到關機訊號，正在優雅釋放 Socket 資源並退出...");
                e.Cancel = true;
                cts.Cancel();
            };

            var devices = new[] { "DEV_01", "DEV_02", "DEV_03", "DEV_04", "DEV_05" };
            var tasks = new Task[devices.Length];

            for (int i = 0; i < devices.Length; i++)
            {
                string devId = devices[i];
                tasks[i] = Task.Run(() => RunDeviceLoopAsync(devId, MqttBrokerIp, MqttBrokerPort, cts.Token));
            }

            try
            {
                await Task.WhenAll(tasks);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("👋 設備端模擬服務已安全停止。");
            }
        }

        private static async Task RunDeviceLoopAsync(string deviceId, string brokerIp, int port, CancellationToken ct)
        {
            var rand = new Random(deviceId.GetHashCode() ^ Environment.TickCount);
            string[] statuses = { "RUNNING", "IDLE", "PAUSED", "ALARM" };

            // 物理感測數值狀態
            double currentTemp = 35.0 + rand.NextDouble() * 10.0;
            double currentBattery = 80.0 + rand.NextDouble() * 20.0;
            
            int backoffDelayMs = 1000;
            const int maxBackoffDelayMs = 16000;

            // 實例化獨立 MQTT 服務驅動
            var mqttService = new MqttSecsService(brokerIp, port, clientId: $"Device_{deviceId}");

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    // 1. 嘗試連線至 Broker (內建中斷自動重連)
                    await mqttService.StartAsync(ct);
                    backoffDelayMs = 1000; // 連線成功後重置指數退避時間

                    // 2. Telemetry 心跳發送主迴圈
                    while (!ct.IsCancellationRequested)
                    {
                        // 模擬機台動態物理量隨時間演化
                        string currentStatus = statuses[rand.Next(statuses.Length)];
                        int alarmCode = currentStatus == "ALARM" ? rand.Next(101, 105) : 0;
                        currentTemp = Math.Clamp(currentTemp + (rand.NextDouble() - 0.48) * 1.5, 30.0, 75.0);
                        currentBattery = Math.Max(10.0, currentBattery - rand.NextDouble() * 0.5);

                        var packet = new SecsGemMqttPacket
                        {
                            DeviceId = deviceId,
                            Stream = 6,
                            Function = 11,
                            Ceid = "CEID_1001",
                            Data = new
                            {
                                Status = currentStatus,
                                Temperature = Math.Round(currentTemp, 2),
                                Battery = Math.Round(currentBattery, 1),
                                AlarmCode = alarmCode
                            },
                            Timestamp = DateTime.Now
                        };

                        string topic = $"fab/{deviceId}/secs/s6f11";
                        await mqttService.PublishSecsEventAsync(topic, packet);

                        Console.WriteLine($"[Publish] [{DateTime.Now:HH:mm:ss}] [{deviceId}] ➜ {topic} | Status:{currentStatus,-7} | Temp:{currentTemp:F1}°C");

                        // 3. 加入訊號發射微抖動 (Jitter)，避免多台機台同時發起 Publish 導致網路封包壅塞
                        int jitterInterval = rand.Next(2500, 4000);
                        await Task.Delay(jitterInterval, ct);
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"⚠️ [{deviceId}] 通訊中斷 ({ex.Message})，{backoffDelayMs / 1000} 秒後重新連線...");
                    await Task.Delay(backoffDelayMs, ct);
                    backoffDelayMs = Math.Min(backoffDelayMs * 2, maxBackoffDelayMs); // 指數退避 (Exponential Backoff)
                }
            }
        }
    }
}
