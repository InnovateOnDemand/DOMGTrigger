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
          public static async Task Run([TimerTrigger("*/20 * * * * *")] TimerInfo myTimer, ILogger log)
          {
               HttpClient client = new HttpClient();

               string url = "https://omgdev.azurewebsites.net/api/Logs/Create";

			HttpResponseMessage response = await client. PostAsJsonAsync(url, new Log { 
                    Message = "Log message from the trigger function " + DateTime.Now.ToString("hh:mm:ss") 
               });

               log.LogInformation(await response.Content.ReadAsStringAsync());
		}
     }
}
