using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;

using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Trigger
{
    public static class FBAudienceReplace
    {
        [FunctionName("ReplaceFacebookAudienceFunction")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)]
            HttpRequest req,
            ILogger log)
        {
            // Email address for notifications.
            const string notificationEmail = "lenin.carrasco@innovateod.com";

            // Tomamos el connection string de Storage desde variables de entorno
            string storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");

            try
            {
                log.LogInformation("===== ReplaceFacebookAudienceFunction START =====");

                // 1. Leer payload del body
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                var payload = JsonConvert.DeserializeObject<ReplaceAudiencePayload>(requestBody);

                // Validaciones mínimas
                if (string.IsNullOrEmpty(payload.AudienceId) ||
                    string.IsNullOrEmpty(payload.FacebookAccessToken) ||
                    string.IsNullOrEmpty(payload.ContainerName) ||
                    payload.BlobPaths == null || payload.BlobPaths.Count == 0)
                {
                    return new BadRequestObjectResult("Faltan datos en el payload (AudienceId, FacebookAccessToken, ContainerName, BlobPaths).");
                }

                string audienceId = payload.AudienceId;
                string fbAccessToken = payload.FacebookAccessToken;
                string containerName = payload.ContainerName;
                var blobPaths = payload.BlobPaths;

                log.LogInformation($"Replace audienceId: {audienceId}, containerName: {containerName}, totalBlobs: {blobPaths.Count}");

                // 2. Descargamos y combinamos TODOS los datos en una sola lista
                var allCustomers = new List<Dictionary<string, object>>();

                BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnectionString);
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);

                foreach (var blobPath in blobPaths)
                {
                    var blobClient = containerClient.GetBlobClient(blobPath);

                    if (!await blobClient.ExistsAsync())
                    {
                        log.LogWarning($"El blob {blobPath} no existe. Se omite.");
                        continue;
                    }

                    // Descargar contenido
                    var downloadResult = await blobClient.DownloadContentAsync();
                    var jsonContent = downloadResult.Value.Content.ToString();

                    // Deserializa
                    var customersChunk = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(jsonContent);
                    if (customersChunk != null && customersChunk.Count > 0)
                    {
                        allCustomers.AddRange(customersChunk);
                    }
                }

                if (allCustomers.Count == 0)
                {
                    log.LogWarning("No se encontró data de clientes en los blobs (o estaban vacíos).");
                    // Eliminamos blobs y salimos
                    foreach (var blobPath in blobPaths)
                    {
                        await containerClient.GetBlobClient(blobPath)
                            .DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots);
                    }
                    return new OkObjectResult("No customer data found to replace.");
                }

                // 3. Preparamos la llamada a /usersreplace? en UNA sola sesión
                string replaceUsersApiUrl = $"https://graph.facebook.com/v20.0/{audienceId}/usersreplace?access_token={fbAccessToken}";

                // Diccionario para acumular resultados
                var audienceUpdates = new Dictionary<string, object>
                {
                    { "session_id", "" },
                    { "num_received", 0 },
                    { "num_invalid_entries", 0 },
                    { "invalid_entry_samples", new JArray() }
                };

                // Mismo sessionId para TODOS los sub-lotes
                long sessionId = GenerateUniqueSessionId();

                // 4. Llamadas en sub-lotes (batch_seq incrementa)
                int chunkSize = 5000;
                int batchSeq = 1;

                using (var httpClient = new HttpClient())
                {
                    for (int i = 0; i < allCustomers.Count; i += chunkSize)
                    {
                        // Sub-lote
                        var subChunk = allCustomers.Skip(i).Take(chunkSize).ToList();

                        // Preparar data
                        var dataForFacebook = PrepareDataForFacebook(subChunk);

                        var payloadData = new
                        {
                            schema = new[]
                            {
                                "EMAIL", "EMAIL", "EMAIL",
                                "PHONE", "PHONE", "PHONE",
                                "FN", "LN", "ZIP",
                                "CT", "ST", "COUNTRY",
                                "DOBY", "GEN"
                            },
                            data = dataForFacebook
                        };

                        bool isLastBatch = (i + chunkSize >= allCustomers.Count);

                        var sessionObj = new
                        {
                            session_id = sessionId,
                            batch_seq = batchSeq,
                            last_batch_flag = isLastBatch,
                            estimated_num_total = allCustomers.Count
                        };

                        using (var content = new MultipartFormDataContent())
                        {
                            content.Add(new StringContent(JsonConvert.SerializeObject(payloadData)), "payload");
                            content.Add(new StringContent(JsonConvert.SerializeObject(sessionObj)), "session");

                            // Invocar Facebook
                            var response = await httpClient.PostAsync(replaceUsersApiUrl, content);
                            if (!response.IsSuccessStatusCode)
                            {
                                var errorContent = await response.Content.ReadAsStringAsync();
                                throw new HttpRequestException($"Facebook API Error: {errorContent}");
                            }

                            var responseContent = await response.Content.ReadAsStringAsync();
                            var result = JsonConvert.DeserializeObject<JObject>(responseContent);

                            // Actualizar contadores
                            audienceUpdates["session_id"] = result["session_id"]?.ToString();
                            audienceUpdates["num_received"] = (int)audienceUpdates["num_received"] + (int)result["num_received"];
                            audienceUpdates["num_invalid_entries"] = (int)audienceUpdates["num_invalid_entries"] + (int)result["num_invalid_entries"];
                            ((JArray)audienceUpdates["invalid_entry_samples"]).Merge(result["invalid_entry_samples"]);
                        }

                        batchSeq++;
                    }
                }

                // 5. Borrar blobs asociados
                log.LogInformation("Eliminando blobs asociados a la audiencia...");
                foreach (var blobPath in blobPaths)
                {
                    var blobClient = containerClient.GetBlobClient(blobPath);
                    await blobClient.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots);
                }

                // 6. Notificar por correo
                var summary = $"num_received: {audienceUpdates["num_received"]}, " +
                              $"num_invalid_entries: {audienceUpdates["num_invalid_entries"]}";
                await SendMail(notificationEmail,
                    "Reemplazo de audiencia completado",
                    $"Se ha completado el proceso de REPLACE para la audiencia {audienceId}.\n{summary}");

                log.LogInformation("===== ReplaceFacebookAudienceFunction END =====");

                return new OkObjectResult(audienceUpdates);
            }
            catch (Exception ex)
            {
                // Manejo de error: intentar limpiar blobs y notificar
                await HandleErrorAndCleanUpBlobs(ex, req);
                return new ObjectResult($"Error en ReplaceFacebookAudienceFunction: {ex.Message}")
                {
                    StatusCode = StatusCodes.Status500InternalServerError
                };
            }
        }

        // En caso de error, intentamos re-leer el payload para saber los blobs que limpiar.
        private static async Task HandleErrorAndCleanUpBlobs(Exception ex, HttpRequest req)
        {
            try
            {
                req.Body.Position = 0; // reset stream
                var requestBody2 = await new StreamReader(req.Body).ReadToEndAsync();
                var payload2 = JsonConvert.DeserializeObject<ReplaceAudiencePayload>(requestBody2);

                if (payload2 != null
                    && !string.IsNullOrEmpty(payload2.ContainerName)
                    && payload2.BlobPaths != null
                    && payload2.BlobPaths.Count > 0)
                {
                    var blobServiceClient2 = new BlobServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
                    var containerClient2 = blobServiceClient2.GetBlobContainerClient(payload2.ContainerName);

                    foreach (var blobPath in payload2.BlobPaths)
                    {
                        await containerClient2.GetBlobClient(blobPath)
                            .DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots);
                    }
                }
            }
            catch
            {
                // Silencio cualquier error adicional
            }

            // Enviar correo de error
            await SendMail("notificaciones@tudominio.com",
                "Error en ReplaceFacebookAudienceFunction",
                $"Mensaje: {ex.Message}\nStackTrace:\n{ex.StackTrace}");
        }

        // Genera session_id único
        private static long GenerateUniqueSessionId()
        {
            return DateTime.UtcNow.Ticks;
        }

        // Convierte la data a la estructura requerida por FB
        private static List<List<string>> PrepareDataForFacebook(List<Dictionary<string, object>> subChunk)
        {
            var finalList = new List<List<string>>();

            foreach (var customer in subChunk)
            {
                var row = new List<string>
                {
                    customer.ContainsKey("Email1") ? customer["Email1"]?.ToString() : "",
                    customer.ContainsKey("Email2") ? customer["Email2"]?.ToString() : "",
                    customer.ContainsKey("Email3") ? customer["Email3"]?.ToString() : "",
                    customer.ContainsKey("Phone1") ? customer["Phone1"]?.ToString() : "",
                    customer.ContainsKey("Phone2") ? customer["Phone2"]?.ToString() : "",
                    customer.ContainsKey("Phone3") ? customer["Phone3"]?.ToString() : "",
                    customer.ContainsKey("FirstName") ? customer["FirstName"]?.ToString() : "",
                    customer.ContainsKey("LastName") ? customer["LastName"]?.ToString() : "",
                    customer.ContainsKey("Zip") ? customer["Zip"]?.ToString() : "",
                    customer.ContainsKey("City") ? customer["City"]?.ToString() : "",
                    customer.ContainsKey("State") ? customer["State"]?.ToString() : "",
                    customer.ContainsKey("Country") ? customer["Country"]?.ToString() : "",
                    customer.ContainsKey("DOBYear") ? customer["DOBYear"]?.ToString() : "",
                    customer.ContainsKey("Gender") ? customer["Gender"]?.ToString() : "",
                };

                finalList.Add(row);
            }
            return finalList;
        }

        // "Fake" para enviar correo
        private static Task SendMail(string recipient, string subject, string body)
        {
            Console.WriteLine("=== [ReplaceFacebookAudienceFunction] Enviando correo ===");
            Console.WriteLine($"To: {recipient}");
            Console.WriteLine($"Subject: {subject}");
            Console.WriteLine($"Body: {body}");
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Modelo para recibir parámetros en ReplaceFacebookAudienceFunction
    /// </summary>
    public class ReplaceAudiencePayload
    {
        public string AudienceId { get; set; }
        public string FacebookAccessToken { get; set; }

        // Blob container y paths donde se guardaron los datos
        public string ContainerName { get; set; }
        public List<string> BlobPaths { get; set; }
    }
}
