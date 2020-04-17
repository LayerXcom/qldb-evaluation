using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.KinesisEvents;
using Amazon.Lambda.Serialization.Json;
using Amazon.QLDB;
using Amazon.QLDB.Driver;
using Amazon.QLDB.Model;
using Amazon.QLDBSession.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
// [assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace QLDB
{
    public class Function
    {
        [LambdaSerializer(typeof(JsonSerializer))]
        public async Task<string> FunctionHandler(KinesisEvent kinesisEvent)
        {
            foreach (var record in kinesisEvent.Records)
            {
                LambdaLogger.Log(GetRecordContents(record.Kinesis));
            }

            var builder = PooledQldbDriver.Builder();
            var driver = builder.WithLedger("demo-ledger-1").Build();
            var session = driver.GetSession();
            
            return kinesisEvent.ToString();
        }

        private string GetRecordContents(KinesisEvent.Record streamRecord)
        {
            using (var reader = new StreamReader(streamRecord.Data, Encoding.UTF8))
            {
                return Regex.Replace(
                    Regex.Replace(
                        reader.ReadToEnd(),
                        "^.*INSERT",
                        "INSERT"),
                    ";.*$",
                    ";");
            }
        }
    }
}
