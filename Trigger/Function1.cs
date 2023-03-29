using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using static System.Net.WebRequestMethods;
using static Trigger.Function1;

namespace Trigger
{
     public static class Function1
     {
          public class Log
          {
               public Log(string _Message)
               {
                    Message = _Message;
               }
               public string Message { get; set; }
          }

          public class Dealer
          {
              public string dealerID { get; set; }
              public string dataProviderID { get; set; }
          }

          [FunctionName("Function1")]
          public static async Task Run([TimerTrigger("0 */10 12-14 * * *")] TimerInfo myTimer, ILogger log)          
          {
            log.LogInformation($"Upload process starts at {DateTime.Now.ToString("hh:mm:ss")}...");
            try
            {                
                HttpClient client = new HttpClient();
                client.Timeout = TimeSpan.FromMinutes(10);

                //string[] baseUrls = new string[] { "https://omgdev.azurewebsites.net/", "https://omgprod.azurewebsites.net/" };
                string baseUrl = "https://omgdev.azurewebsites.net/";
                if (DateTime.Now.Hour == 12)
                {
                    baseUrl = "https://omgprod.azurewebsites.net/";
                    log.LogInformation("Processing records for the Production Environment");
                }
                else { log.LogInformation("Processing records for the Development Environment"); }

                string logsUrl = $"{baseUrl}api/Logs/Create";

                //Call the endpoint to get all the dealers without processing finished today
                HttpResponseMessage dealersResponse = await client.GetAsync($"{baseUrl}DealersWithoutUploadProcessToday");

                if (dealersResponse.StatusCode != HttpStatusCode.OK)
                {
                    log.LogError($"There was an error on the call to api/DealersWithoutUploadProcessToday");
                }
                else
                {
                    var dealersJson = await dealersResponse.Content.ReadAsStringAsync();
                    var dealers = JsonConvert.DeserializeObject<List<Dealer>>(dealersJson);

                    if (dealers.Count > 0)
                    {
                        var urlUpdateHoldRecords = $"{baseUrl}DataFiles/UpdateHoldRecords";
                        HttpResponseMessage firstResponse = await client.GetAsync(urlUpdateHoldRecords);
                        if (firstResponse.StatusCode != HttpStatusCode.OK)
                        {
                            log.LogError($"There was an error on the call to the EndPoint: DataFiles/UpdateHoldRecords");
                        }
                        else
                        {
                            var stringRes = await firstResponse.Content.ReadAsStringAsync();
                            string logMessage = $"EndPoint: DataFiles/UpdateHoldRecords called with result: {stringRes}";
                            await client.PostAsJsonAsync(logsUrl, new Log(logMessage));
                            log.LogInformation(logMessage);
                        }

                        foreach (var dealer in dealers)
                        {
                            var url = $"{baseUrl}DataFiles/DataFileUpload?DataProviderID={dealer.dataProviderID}&dealerIds={dealer.dealerID}";

                            HttpResponseMessage response = await client.PostAsJsonAsync(url, new { });

                            if (response.StatusCode != HttpStatusCode.OK)
                            {
                                log.LogError($"There was an error on the call");
                            }
                            else
                            {
                                var stringRes = await response.Content.ReadAsStringAsync();
                                string logMessage = $"Upload Information Process called with result: {stringRes} for Dealer: {dealer.dealerID} and DataProvider: {dealer.dataProviderID} ";
                                await client.PostAsJsonAsync(logsUrl, new Log(logMessage));
                                log.LogInformation(logMessage);
                            }
                        }
                    }
                    else
                    {
                        log.LogInformation("No more dealers pending to upload...");
                    }
                }
            }
            catch (Exception ex)
               {
			        log.LogError(ex.Message);
               }
		}
     }
}
