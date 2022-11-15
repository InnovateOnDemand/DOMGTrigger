using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System;
using System.Net.Http;
using System.Threading.Tasks;
using static System.Net.WebRequestMethods;

namespace Trigger
{
     public static class Function1
     {
          public class Log
          {    
               public string Message { get; set; }
          }

          [FunctionName("Function1")]
          public static async Task Run([TimerTrigger("0 30 3 * * *")] TimerInfo myTimer, ILogger log)
          {
               HttpClient client = new HttpClient();

               string baseUrl = "https://omgdev.azurewebsites.net/api/";

			string[] urls = new string[]{
				$"{baseUrl}DataFiles/UpdateHoldRecords",
				$"{baseUrl}DataFiles/DataFileUpload"
			};

               foreach (var url in urls)
               {
			     HttpResponseMessage response = await client.GetAsync(baseUrl + url);
                    log.LogInformation(await response.Content.ReadAsStringAsync());                    
               }
		}
     }
}
