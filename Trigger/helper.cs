using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Newtonsoft.Json;

namespace Trigger
{
    public static class helper
    {
        /// <summary>
        /// Converts the list of dictionaries into the structure expected by Facebook.
        /// </summary>
        public static List<List<string>> PrepareDataForFacebook(List<Dictionary<string, object>> subChunk)
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

        /// <summary>
        /// Function to delete the blobs in case of errors.
        /// </summary>
        public static async Task HandleErrorAndCleanUpBlobs(Exception ex, string message)
        {
            try
            {
                var payload = JsonConvert.DeserializeObject<PopulateAudiencePayload>(message);

                if (payload != null
                    && !string.IsNullOrEmpty(payload.ContainerName)
                    && payload.BlobPaths != null
                    && payload.BlobPaths.Count > 0)
                {
                    var blobServiceClient2 = new BlobServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
                    var containerClient2 = blobServiceClient2.GetBlobContainerClient(payload.ContainerName);

                    foreach (var blobPath in payload.BlobPaths)
                    {
                        await containerClient2.GetBlobClient(blobPath)
                            .DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots);
                    }
                }
            }
            catch
            {
                // Silence any additional exceptions
            }
        }

        /// <summary>
        /// Function to send email.
        /// </summary>
        public static async Task<bool> SendMail(string recipient, string subject, string body)
        {
            Console.WriteLine("=== ENVIANDO CORREO ===");
            Console.WriteLine($"To: {recipient}");
            Console.WriteLine($"Subject: {subject}");
            Console.WriteLine($"Body:\n{body}");

            // Encode the parameters for the URL
            string encodedRecipient = Uri.EscapeDataString(recipient);
            string encodedSubject = Uri.EscapeDataString(subject);
            string encodedBody = Uri.EscapeDataString(body);

            // Construct the URL
            string url = $"https://omgdev.azurewebsites.net/SendEmail?emailTo={encodedRecipient}&subject={encodedSubject}&bodymessage={encodedBody}";

            // Create an HttpClient instance
            using (HttpClient client = new HttpClient())
            {
                try
                {
                    // Send a GET request to the endpoint
                    HttpResponseMessage response = await client.GetAsync(url);

                    // Check if the request was successful
                    if (response.IsSuccessStatusCode)
                    {
                        Console.WriteLine("Email sent successfully!");
                        return true; // Indicate success
                    }
                    else
                    {
                        Console.WriteLine($"Email sending failed with status code: {response.StatusCode}");
                        string responseContent = await response.Content.ReadAsStringAsync();
                        Console.WriteLine($"Response content: {responseContent}");
                        return false; // Indicate failure
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"An error occurred while sending the email: {ex.Message}");
                    return false; // Indicate failure due to exception
                }
            }
        }
    }
}
