using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

using Newtonsoft.Json;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Queues;

namespace Trigger
{
    public static class FBAudienceExtract
    {
        [FunctionName("ExtractBigQueryDataFunction")]
        public static async Task Run(
            [QueueTrigger("extract-queue", Connection = "AzureWebJobsStorage")]
            string message,
            ILogger log)
        {
            log.LogInformation("===== ExtractBigQueryDataFunction START =====");

            // 1. Deserializar el mensaje
            var payload = JsonConvert.DeserializeObject<ExtractAudiencePayload>(message);

            // 2. Extraer data de BigQuery
            var customerData = GetCustomerDataFromBigQuery(
                payload.Sql, payload.SqlSales, payload.SqlService, log);

            if (customerData.Count == 0)
            {
                log.LogInformation("No data found from BigQuery. Exiting function...");
                return;
            }

            // 3. Guardar data en Blob
            string storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            string containerName = string.IsNullOrEmpty(payload.ContainerName)
                                    ? "fb-audiences-data"
                                    : payload.ContainerName;

            var blobPaths = await SaveCustomerDataToBlobAsync(
                payload.AudienceId,
                customerData,
                containerName,
                storageConnectionString);

            log.LogInformation($"Total records: {customerData.Count}, Blobs created: {blobPaths.Count}");

            if (blobPaths.Count == 0)
            {
                log.LogInformation("No valid blobs created. Possibly no data. Exiting function...");
                return;
            }

            // 4. Encolar mensaje para la Function "Populate" o "Replace"
            //    segun IsReplace
            string nextQueueName = payload.IsReplace ? "replace-queue" : "populate-queue";

            QueueClient queueClient = new QueueClient(storageConnectionString, nextQueueName);
            await queueClient.CreateIfNotExistsAsync();

            var nextPayload = new PopulateQueuePayload
            {
                AudienceId = payload.AudienceId,
                FacebookAccessToken = payload.FacebookAccessToken,
                ContainerName = containerName,
                BlobPaths = blobPaths
            };

            string nextJson = JsonConvert.SerializeObject(nextPayload);
            await queueClient.SendMessageAsync(nextJson);

            log.LogInformation($"Message enqueued to {nextQueueName}. Done extraction.");
        }

        private static List<Dictionary<string, object>> GetCustomerDataFromBigQuery(
            string sql, string sqlSales, string sqlService, ILogger log)
        {
            log.LogInformation("Extracting data from BigQuery...");

            // Combinar 'sql', 'sqlSales', 'sqlService' si fuera necesario.
            // Ejemplo: asumiendo `sql` es tu WHERE principal. 
            // Haz tus validaciones.

            // 1. Construir query
            string query = $@"
                SELECT 
                  max(E.EMAIL1) as email1, 
                  max(E.EMAIL2) as email2, 
                  max(E.EMAIL3) as email3, 
                  max(C.PHONE) as phone1, 
                  max(C.PHONE2) as phone2, 
                  max(C.PHONE3) as phone3, 
                  max(C.FNAME) as fn, 
                  max(C.LNAME) as ln, 
                  max(C.ZIP) as zip, 
                  max(C.CITY) as ct, 
                  max(C.STATE) as st,
                  max(C.DOB) as dob, 
                  max(C.GENDER) as gen, 
                  max(C.AGE) as age, 
                  left(max(C.DOB),4) as doby, 
                  '' as uid,  
                  '' as madid, 
                  'US' as country
                FROM `infutor-tci-auto-email.infutor_data.auto` as A 
                inner join `infutor-tci-auto-email.infutor_data.email` as E ON A.PID = E.PID 
                inner join `infutor-tci-auto-email.infutor_data.consumer` as C on C.PID = A.PID 
                WHERE 1=1
                {sql}
                group by c.pid
            ";

            // 2. Credenciales
            //var jsonCredentialFileApi = Path.Combine(Environment.CurrentDirectory, "infutor-tci-auto-email-a2833fed2f8a.json");
            //var credential = GoogleCredential.FromFile(jsonCredentialFileApi);
            string jsonCreds = Environment.GetEnvironmentVariable("GOOGLE_CREDENTIALS_JSON");
            var credential = GoogleCredential.FromJson(jsonCreds);


            // 3. Crear cliente
            var client = BigQueryClient.Create("infutor-tci-auto-email", credential);

            // 4. Ejecutar query
            BigQueryResults results = client.ExecuteQuery(query, parameters: null);

            // 5. Mapear a List<Dictionary<string, object>>
            var customerData = new List<Dictionary<string, object>>();
            foreach (var row in results)
            {
                var dict = new Dictionary<string, object>
                {
                    { "email1", row["email1"]?.ToString() },
                    { "email2", row["email2"]?.ToString() },
                    { "email3", row["email3"]?.ToString() },
                    { "phone1", row["phone1"]?.ToString() },
                    { "phone2", row["phone2"]?.ToString() },
                    { "phone3", row["phone3"]?.ToString() },
                    { "fn", row["fn"]?.ToString() },
                    { "ln", row["ln"]?.ToString() },
                    { "zip", row["zip"]?.ToString() },
                    { "ct", row["ct"]?.ToString() },
                    { "st", row["st"]?.ToString() },
                    { "country", row["country"]?.ToString() },
                    { "doby", row["doby"]?.ToString() },
                    { "gen", row["gen"]?.ToString() }
                };
                customerData.Add(dict);
            }

            return customerData;
        }

        private static async Task<List<string>> SaveCustomerDataToBlobAsync(
            string audienceId,
            List<Dictionary<string, object>> customerData,
            string containerName,
            string storageConnectionString)
        {
            var blobPaths = new List<string>();
            var blobServiceClient = new BlobServiceClient(storageConnectionString);
            var containerClient = blobServiceClient.GetBlobContainerClient(containerName);

            await containerClient.CreateIfNotExistsAsync(PublicAccessType.None);

            int maxPerFile = 50000;
            if (customerData.Count <= maxPerFile)
            {
                string blobName = $"{audienceId}/{audienceId}.json";
                var blobClient = containerClient.GetBlobClient(blobName);

                string json = JsonConvert.SerializeObject(customerData);
                using (var ms = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)))
                {
                    await blobClient.UploadAsync(ms, overwrite: true);
                }

                blobPaths.Add(blobName);
            }
            else
            {
                int chunkSize = maxPerFile;
                int total = customerData.Count;
                int index = 0;
                int fileNum = 1;

                while (index < total)
                {
                    var chunk = customerData.Skip(index).Take(chunkSize).ToList();
                    string blobName = $"{audienceId}/{audienceId}_chunk{fileNum}.json";
                    var blobClient = containerClient.GetBlobClient(blobName);

                    string json = JsonConvert.SerializeObject(chunk);
                    using (var ms = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)))
                    {
                        await blobClient.UploadAsync(ms, overwrite: true);
                    }
                    blobPaths.Add(blobName);

                    index += chunkSize;
                    fileNum++;
                }
            }
            return blobPaths;
        }
    }

    // El payload que recibimos en la cola "extract-queue"
    public class ExtractAudiencePayload
    {
        public string AudienceId { get; set; }
        public string Sql { get; set; }
        public string SqlSales { get; set; }
        public string SqlService { get; set; }
        public string FacebookAccessToken { get; set; }
        public bool IsReplace { get; set; }
        public string ContainerName { get; set; }
    }

    // El payload que enviamos a la cola de "populate" o "replace"
    public class PopulateQueuePayload
    {
        public string AudienceId { get; set; }
        public string FacebookAccessToken { get; set; }
        public string ContainerName { get; set; }
        public List<string> BlobPaths { get; set; }
    }
}
