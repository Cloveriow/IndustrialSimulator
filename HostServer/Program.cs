using System;
using System.Data;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using IndustrialCommunication;
using Oracle.ManagedDataAccess.Client;

namespace HostServer
{
    class Program
    {
        private static readonly string DbHost = Environment.GetEnvironmentVariable("DB_HOST") ?? "172.20.0.2";
        private static readonly string DbConnStr = 
            $"User Id=system;Password=YourPassword123;Data Source={DbHost}:1521/FREEPDB1;";

        private static readonly string MqttBrokerIp = Environment.GetEnvironmentVariable("MQTT_HOST") ?? "172.20.0.10";

        static async Task Main(string[] args)
        {
            Console.WriteLine("==================================================");
            Console.WriteLine($"🖥️ [HOST SERVER - SECS/GEM over MQTT Edition]");
            Console.WriteLine($"   DB Target: {DbHost}:1521 | MQTT Broker: {MqttBrokerIp}:1883");
            Console.WriteLine("==================================================");

            using var cts = new CancellationTokenSource();

            // 1. 啟動時發起資料庫初始化與建表 (含自動重試機制)
            await InitDatabaseWithRetryAsync();

            // 2. 初始化 MQTT 通訊服務
            var mqttService = new MqttSecsService(MqttBrokerIp, clientId: "HostServer_Master");

            // 3. 註冊收到 SECS/GEM 事件時寫入 Oracle DB
            mqttService.OnSecsMessageReceived += async (packet) =>
            {
                Console.WriteLine($"📩 [HOST Recv SECS] S{packet.Stream}F{packet.Function} | Device: {packet.DeviceId} | CEID: {packet.Ceid}");
                await SaveToDatabaseAsync(packet);
            };

            // 4. 啟動 MQTT 服務並訂閱全廠設備 Topic
            await mqttService.StartAsync(cts.Token);
            await mqttService.SubscribeAsync("fab/+/secs/#");

            await Task.Delay(-1, cts.Token);
        }

        private static async Task InitDatabaseWithRetryAsync()
        {
            int retryCount = 0;
            while (true)
            {
                try
                {
                    using IDbConnection db = new OracleConnection(DbConnStr);

                    string createEqpTable = @"
                        CREATE TABLE EquipmentState (
                            EqpId VARCHAR2(50) PRIMARY KEY,
                            ControlState VARCHAR2(20) NOT NULL,
                            ProcessingState VARCHAR2(20) NOT NULL,
                            E84State VARCHAR2(20) NOT NULL,
                            AlarmStatus NUMBER DEFAULT 0,
                            LastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )";

                    string createLogTable = @"
                        CREATE TABLE EquipmentStateLog (
                            LogId NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            EqpId VARCHAR2(50),
                            OldState VARCHAR2(50),
                            NewState VARCHAR2(50),
                            TriggerSource VARCHAR2(50),
                            LogTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )";

                    try { await db.ExecuteAsync(createEqpTable); } catch (OracleException ex) when (ex.Number == 955) { }
                    try { await db.ExecuteAsync(createLogTable); } catch (OracleException ex) when (ex.Number == 955) { }

                    Console.WriteLine("✅ [DB Initializer] Oracle 資料庫表結構驗證/建立成功！");
                    break;
                }
                catch (Exception ex)
                {
                    retryCount++;
                    Console.WriteLine($"⏳ [DB Pending] 等待 Oracle 資料庫初始化 ({retryCount})... {ex.Message}");
                    await Task.Delay(5000);
                }
            }
        }

        private static async Task SaveToDatabaseAsync(SecsGemMqttPacket packet)
        {
            try
            {
                using IDbConnection db = new OracleConnection(DbConnStr);

                string status = "UNKNOWN";
                int alarmCode = 0;

                // 解析 JSON 格式的 Payload 內容
                if (packet.Data is JsonElement element)
                {
                    if (element.TryGetProperty("Status", out var statusProp))
                        status = statusProp.GetString() ?? "UNKNOWN";
                    if (element.TryGetProperty("AlarmCode", out var alarmProp))
                        alarmCode = alarmProp.GetInt32();
                }

                string mergeSql = @"
                    MERGE INTO EquipmentState t
                    USING (SELECT :EqpId AS EqpId FROM DUAL) s
                    ON (t.EqpId = s.EqpId)
                    WHEN MATCHED THEN
                        UPDATE SET ProcessingState = :Status, AlarmStatus = :AlarmCode, LastUpdated = :Timestamp
                    WHEN NOT MATCHED THEN
                        INSERT (EqpId, ControlState, ProcessingState, E84State, AlarmStatus, LastUpdated)
                        VALUES (:EqpId, 'REMOTE', :Status, 'E84_IDLE', :AlarmCode, :Timestamp)";

                string insertLogSql = @"
                    INSERT INTO EquipmentStateLog (EqpId, NewState, TriggerSource, LogTime)
                    VALUES (:EqpId, :NewState, 'MQTT_SECS', :LogTime)";

                await db.ExecuteAsync(mergeSql, new { EqpId = packet.DeviceId, Status = status, AlarmCode = alarmCode, Timestamp = packet.Timestamp });
                await db.ExecuteAsync(insertLogSql, new { EqpId = packet.DeviceId, NewState = $"{status} (CEID:{packet.Ceid})", LogTime = packet.Timestamp });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ [DB Save Error] 寫入資料庫失敗: {ex.Message}");
            }
        }
    }
}