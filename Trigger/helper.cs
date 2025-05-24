using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
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

            // *** IMPORTANT: Use the correct dictionary keys (lowercase from FBAudienceExtract) ***
            foreach (var customer in subChunk)
            {
                // Normalize and Hash each field before adding
                var row = new List<string>
            {
                // EMAIL x 3
                ComputeSha256Hash(NormalizeEmail(customer.ContainsKey("email1") ? customer["email1"]?.ToString() : "")),
                ComputeSha256Hash(NormalizeEmail(customer.ContainsKey("email2") ? customer["email2"]?.ToString() : "")),
                ComputeSha256Hash(NormalizeEmail(customer.ContainsKey("email3") ? customer["email3"]?.ToString() : "")),
                // PHONE x 3
                ComputeSha256Hash(NormalizePhone(customer.ContainsKey("phone1") ? customer["phone1"]?.ToString() : "")),
                ComputeSha256Hash(NormalizePhone(customer.ContainsKey("phone2") ? customer["phone2"]?.ToString() : "")),
                ComputeSha256Hash(NormalizePhone(customer.ContainsKey("phone3") ? customer["phone3"]?.ToString() : "")),
                // FN (First Name)
                ComputeSha256Hash(NormalizeName(customer.ContainsKey("fn") ? customer["fn"]?.ToString() : "")),
                // LN (Last Name)
                ComputeSha256Hash(NormalizeName(customer.ContainsKey("ln") ? customer["ln"]?.ToString() : "")),
                // ZIP
                ComputeSha256Hash(NormalizeZip(customer.ContainsKey("zip") ? customer["zip"]?.ToString() : "")),
                // CT (City)
                ComputeSha256Hash(NormalizeLocation(customer.ContainsKey("ct") ? customer["ct"]?.ToString() : "")),
                // ST (State)
                ComputeSha256Hash(NormalizeLocation(customer.ContainsKey("st") ? customer["st"]?.ToString() : "")),
                // COUNTRY
                ComputeSha256Hash(customer.ContainsKey("country") ? customer["country"]?.ToString().ToLowerInvariant() : ""), // Country code should already be 2 letters, just lowercase
                // DOBY (Date of Birth Year)
                ComputeSha256Hash(customer.ContainsKey("doby") ? customer["doby"]?.ToString() : ""), // Assuming YYYY format from query
                // GEN (Gender)
                ComputeSha256Hash(NormalizeGender(customer.ContainsKey("gen") ? customer["gen"]?.ToString() : ""))
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

        // Temporary function to send email only to the admins. (Currently disabled)
        //public static async Task<bool> SendMail(string recipient, string subject, string body)
        //{
        //    Console.WriteLine("=== ENVIANDO CORREO ===");
        //    Console.WriteLine($"To: {recipient}");
        //    Console.WriteLine($"Subject: {subject}");
        //    Console.WriteLine($"Body:\n{body}");

        //    body = body + $"\n Operation started by user: {recipient}";

        //    // Encode the parameters for the URL
        //    string encodedRecipient = Uri.EscapeDataString("jacob.gomez@innovateod.com");
        //    string encodedSubject = Uri.EscapeDataString(subject);
        //    string encodedBody = Uri.EscapeDataString(body);

        //    // Construct the URL
        //    string url = $"https://omgdev.azurewebsites.net/SendEmail?emailTo={encodedRecipient}&subject={encodedSubject}&bodymessage={encodedBody}";

        //    // Create an HttpClient instance
        //    using (HttpClient client = new HttpClient())
        //    {
        //        try
        //        {
        //            // Send a GET request to the endpoint
        //            HttpResponseMessage response = await client.GetAsync(url);

        //            // Check if the request was successful
        //            if (response.IsSuccessStatusCode)
        //            {
        //                Console.WriteLine("Email sent successfully!");
        //                //return true; // Indicate success
        //            }
        //            else
        //            {
        //                Console.WriteLine($"Email sending failed with status code: {response.StatusCode}");
        //                string responseContent = await response.Content.ReadAsStringAsync();
        //                Console.WriteLine($"Response content: {responseContent}");
        //                //return false; // Indicate failure
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            Console.WriteLine($"An error occurred while sending the email to Jacob: {ex.Message}");
        //            //return false; // Indicate failure due to exception
        //        }
        //    }

        //    encodedRecipient = Uri.EscapeDataString("lenin.carrasco@innovateod.com");
        //    url = $"https://omgdev.azurewebsites.net/SendEmail?emailTo={encodedRecipient}&subject={encodedSubject}&bodymessage={encodedBody}";

        //    // Create an HttpClient instance
        //    using (HttpClient client = new HttpClient())
        //    {
        //        try
        //        {
        //            // Send a GET request to the endpoint
        //            HttpResponseMessage response = await client.GetAsync(url);

        //            // Check if the request was successful
        //            if (response.IsSuccessStatusCode)
        //            {
        //                Console.WriteLine("Email sent successfully!");
        //                return true; // Indicate success
        //            }
        //            else
        //            {
        //                Console.WriteLine($"Email sending failed with status code: {response.StatusCode}");
        //                string responseContent = await response.Content.ReadAsStringAsync();
        //                Console.WriteLine($"Response content: {responseContent}");
        //                return false; // Indicate failure
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            Console.WriteLine($"An error occurred while sending the email: {ex.Message}");
        //            return false; // Indicate failure due to exception
        //        }
        //    }

        //    encodedRecipient = Uri.EscapeDataString("keith@dealeromg.com");
        //    url = $"https://omgdev.azurewebsites.net/SendEmail?emailTo={encodedRecipient}&subject={encodedSubject}&bodymessage={encodedBody}";

        //    // Create an HttpClient instance
        //    using (HttpClient client = new HttpClient())
        //    {
        //        try
        //        {
        //            // Send a GET request to the endpoint
        //            HttpResponseMessage response = await client.GetAsync(url);

        //            // Check if the request was successful
        //            if (response.IsSuccessStatusCode)
        //            {
        //                Console.WriteLine("Email sent successfully!");
        //                return true; // Indicate success
        //            }
        //            else
        //            {
        //                Console.WriteLine($"Email sending failed with status code: {response.StatusCode}");
        //                string responseContent = await response.Content.ReadAsStringAsync();
        //                Console.WriteLine($"Response content: {responseContent}");
        //                return false; // Indicate failure
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            Console.WriteLine($"An error occurred while sending the email: {ex.Message}");
        //            return false; // Indicate failure due to exception
        //        }
        //    }
        //}

        // Helper to compute SHA256 hash 
        private static string ComputeSha256Hash(string rawData)
        {
            if (string.IsNullOrEmpty(rawData))
            {
                return "";
            }

            using (SHA256 sha256Hash = SHA256.Create())
            {
                // ComputeHash - returns byte array
                byte[] bytes = sha256Hash.ComputeHash(Encoding.UTF8.GetBytes(rawData));

                // Convert byte array to a lowercase string
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < bytes.Length; i++)
                {
                    builder.Append(bytes[i].ToString("x2")); // "x2" formats as lowercase hex
                }
                return builder.ToString();
            }
        }

        // Normalization Functions (Examples - refine based on exact needs)
        private static string NormalizeEmail(string email)
        {
            return email?.Trim().ToLowerInvariant() ?? "";
        }

        private static string NormalizePhone(string phone)
        {
            // Remove non-digits, maybe handle country codes if needed
            return Regex.Replace(phone ?? "", @"\D", "");
        }

        private static string NormalizeName(string name)
        {
            // Lowercase, remove punctuation (basic example)
            return Regex.Replace(name?.ToLowerInvariant() ?? "", @"[^a-z]", "");
        }

        private static string NormalizeLocation(string loc)
        {
            // Lowercase, remove punctuation/spaces (basic example)
            return Regex.Replace(loc?.ToLowerInvariant() ?? "", @"[^a-z]", "");
        }
        private static string NormalizeZip(string zip)
        {
            // Lowercase, remove spaces. Add logic for US 5-digit if necessary.
            return zip?.ToLowerInvariant().Replace(" ", "") ?? "";
        }

        private static string NormalizeGender(string gender)
        {
            string lowerGender = gender?.ToLowerInvariant();
            return (lowerGender == "m" || lowerGender == "f") ? lowerGender : "";
        }
    }
}
