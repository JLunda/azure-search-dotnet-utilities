// This is a prototype tool that allows for extraction of data from a search index
// Since this tool is still under development, it should not be used for production usage

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Identity;
using Azure.Search.Documents;
using Azure.Search.Documents.Indexes;
using Azure.Search.Documents.Indexes.Models;
using Azure.Search.Documents.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Identity.Client;

namespace AzureSearchBackupRestoreIndex;

class Program
{
    private static AzureCloudInstance AzureCloudInstance = AzureCloudInstance.AzurePublic;
    private static string SearchServiceDNSSuffix;
    private static string SourceSearchServiceName;
    private static string SourceAdminKey;
    private static string SourceIndexName;
    private static string TargetSearchServiceName;
    private static string TargetAdminKey;
    private static string TargetIndexName;
    private static string BackupDirectory;
    private static AuthenticationMethodEnum AuthenticationMethod;
    private static IndexCopyModeEnum IndexCopyMode;
    private static DefaultAzureCredentialOptions DefaultAzureCredentialOptions;
    private static DefaultAzureCredential DefaultAzureCredential;

    private static SearchIndexClient SourceIndexClient;
    private static SearchClient SourceSearchClient;
    private static SearchIndexClient TargetIndexClient;
    private static SearchClient TargetSearchClient;

    private static int MaxBatchSize = 500;          // JSON files will contain this many documents / file and can be up to 1000
    private static int ParallelizedJobs = 10;       // Output content in parallel jobs

    static void Main()
    {
        //Get source and target search service info and index names from appsettings.json file
        //Set up source and target search service clients
        ConfigurationSetup();

        //Backup the source index
        Console.WriteLine("\nSTART INDEX BACKUP");
        switch (IndexCopyMode)
        {
            case IndexCopyModeEnum.Single:
                SetupIndexSearchClients(SourceIndexName, TargetIndexName);
                BackupIndexAndDocuments(SourceIndexName);
                //Recreate and import content to target index
                Console.WriteLine("\nSTART INDEX RESTORE");
                DeleteIndex(TargetIndexName);
                CreateTargetIndex(SourceIndexName, TargetIndexName);
                ImportFromJSON(SourceIndexName, TargetIndexName);
                PerformSanityCheck();
                break;

            case IndexCopyModeEnum.All:
                Pageable<SearchIndex> indices = SourceIndexClient.GetIndexes();
                foreach (var index in indices)
                {
                    SetupIndexSearchClients(index.Name, index.Name);
                    BackupIndexAndDocuments(index.Name);
                    Console.WriteLine("\nSTART INDEX RESTORE");
                    DeleteIndex(index.Name);
                    CreateTargetIndex(index.Name, index.Name);
                    ImportFromJSON(index.Name, index.Name);
                    PerformSanityCheck();
                }
                break;
            default:
                throw new Exception("Could not determine Index Copy Mode.");
        }

        Console.WriteLine("Press any key to continue...");
        Console.ReadLine();
    }

    private static void PerformSanityCheck()
    {
        Console.WriteLine("\n  Waiting 10 seconds for target to index content...");
        Console.WriteLine("  NOTE: For really large indexes it may take longer to index all content.\n");
        Thread.Sleep(10000);

        // Validate all content is in target index
        int sourceCount = GetCurrentDocCount(SourceSearchClient);
        int targetCount = GetCurrentDocCount(TargetSearchClient);
        Console.WriteLine("\nSAFEGUARD CHECK: Source and target index counts should match");
        Console.WriteLine(" Source index contains {0} docs", sourceCount);
        Console.WriteLine(" Target index contains {0} docs\n", targetCount);
    }

    static void ConfigurationSetup()
    {
        IConfigurationBuilder builder = new ConfigurationBuilder().AddJsonFile("appsettings.json");
        IConfigurationRoot configuration = builder.Build();

        AzureCloudInstance = Enum.TryParse(configuration["AzureCloud"], ignoreCase: true, out AzureCloudInstance azureCloudInstanceParsed) ? azureCloudInstanceParsed : AzureCloudInstance.AzurePublic;
        SourceSearchServiceName = configuration["SourceSearchServiceName"];
        SourceAdminKey = configuration["SourceAdminKey"];
        SourceIndexName = configuration["SourceIndexName"];
        TargetSearchServiceName = configuration["TargetSearchServiceName"];
        TargetAdminKey = configuration["TargetAdminKey"];
        TargetIndexName = configuration["TargetIndexName"];
        BackupDirectory = configuration["BackupDirectory"];
        AuthenticationMethod = Enum.TryParse(configuration["AuthenticationMethod"], ignoreCase: true, out AuthenticationMethodEnum shouldUseManagedIdentityParsed) ? shouldUseManagedIdentityParsed : AuthenticationMethodEnum.ManagedIdentity;
        IndexCopyMode = Enum.TryParse(configuration["IndexCopyMode"], ignoreCase: true, out IndexCopyModeEnum indexCopyModeParsed) ? indexCopyModeParsed : IndexCopyModeEnum.All;

        Console.WriteLine($$"""
        CONFIGURATION:
            Azure Cloud: {{AzureCloudInstance}}
            Authentication Method: {{AuthenticationMethod}}
            Index Copy Mode: {{IndexCopyMode}}
            Source service: {{SourceSearchServiceName}}
            Source Index: {{(string.IsNullOrWhiteSpace(SourceIndexName) ? "N/A" : SourceIndexName)}}
            Target service: {{TargetSearchServiceName}}
            Target index: {{(string.IsNullOrWhiteSpace(TargetIndexName) ? "N/A" : TargetIndexName)}}
            Backup directory: {{BackupDirectory}}
            Does this look correct? Press any key to continue, Ctrl+C to cancel.");
        """);
        Console.ReadLine();

        DefaultAzureCredentialOptions = new DefaultAzureCredentialOptions();
        SearchClientOptions searchClientOptions = new SearchClientOptions();

        switch (AzureCloudInstance)
        {
            case AzureCloudInstance.AzureUsGovernment:
                DefaultAzureCredentialOptions.AuthorityHost = AzureAuthorityHosts.AzureGovernment;
                SearchServiceDNSSuffix = configuration["Endpoint"] ?? "search.azure.us";
                searchClientOptions.Audience = SearchAudience.AzureGovernment;
                break;

            case AzureCloudInstance.AzurePublic:
            default:
                DefaultAzureCredentialOptions.AuthorityHost = AzureAuthorityHosts.AzurePublicCloud;
                SearchServiceDNSSuffix = configuration["Endpoint"] ?? "search.windows.net";
                searchClientOptions.Audience = SearchAudience.AzurePublicCloud;
                break;
        }

        switch (AuthenticationMethod)
        {
            case AuthenticationMethodEnum.APIKey:
                SourceIndexClient = new SearchIndexClient(new Uri($"https://{SourceSearchServiceName}.{SearchServiceDNSSuffix}"), new AzureKeyCredential(SourceAdminKey), searchClientOptions);

                TargetIndexClient = new SearchIndexClient(new Uri($"https://{TargetSearchServiceName}.{SearchServiceDNSSuffix}"), new AzureKeyCredential(TargetAdminKey), searchClientOptions);
                break;
            case AuthenticationMethodEnum.ManagedIdentity:
            default:
                DefaultAzureCredential = new DefaultAzureCredential(DefaultAzureCredentialOptions);

                SourceIndexClient = new SearchIndexClient(new Uri($"https://{SourceSearchServiceName}.{SearchServiceDNSSuffix}"), DefaultAzureCredential, searchClientOptions);

                TargetIndexClient = new SearchIndexClient(new Uri($"https://{TargetSearchServiceName}.{SearchServiceDNSSuffix}"), DefaultAzureCredential, searchClientOptions);
                break;
        }
    }

    private static void SetupIndexSearchClients(string sourceIndexName, string targetIndexName)
    {
        SourceIndexName = sourceIndexName;
        TargetIndexName = targetIndexName;
        SourceSearchClient = SourceIndexClient.GetSearchClient(sourceIndexName);
        TargetSearchClient = TargetIndexClient.GetSearchClient(targetIndexName);
    }

    static void BackupIndexAndDocuments(string indexName)
    {
        // Backup the index schema to the specified backup directory
        Console.WriteLine("\n Backing up source index schema to {0}\n", Path.Combine(BackupDirectory, indexName + ".schema"));

        File.WriteAllText(Path.Combine(BackupDirectory, indexName + ".schema"), GetIndexSchema(indexName));

        // Extract the content to JSON files
        int SourceDocCount = GetCurrentDocCount(SourceSearchClient);
        WriteIndexDocuments(SourceDocCount, indexName);     // Output content from index to json files
    }

    static void WriteIndexDocuments(int CurrentDocCount, string sourceIndexName)
    {
        // Write document files in batches (per MaxBatchSize) in parallel
        int FileCounter = 0;
        double batchRatio = (double)CurrentDocCount / MaxBatchSize;
        for (int batch = 0; batch <= batchRatio; batch += ParallelizedJobs)
        {
            List<Task> tasks = new List<Task>();
            for (int job = 0; job < ParallelizedJobs; job++)
            {
                FileCounter++;
                int fileCounter = FileCounter;
                if ((fileCounter - 1) * MaxBatchSize < CurrentDocCount)
                {
                    Console.WriteLine(" Backing up source documents to {0} - (batch size = {1})", Path.Combine(BackupDirectory, sourceIndexName + fileCounter + ".json"), MaxBatchSize);

                    tasks.Add(Task.Factory.StartNew(() =>
                        ExportToJSON((fileCounter - 1) * MaxBatchSize, Path.Combine(BackupDirectory, $"{sourceIndexName}{fileCounter}.json"))
                    ));
                }

            }
            Task.WaitAll(tasks.ToArray());  // Wait for all the stored procs in the group to complete
        }

        return;
    }

    static void ExportToJSON(int Skip, string FileName)
    {
        // Extract all the documents from the selected index to JSON files in batches of 500 docs / file
        string json = string.Empty;
        try
        {
            SearchOptions options = new SearchOptions()
            {
                IncludeTotalCount = true,
                SearchMode = SearchMode.All,
                Size = MaxBatchSize,
                Skip = Skip
            };

            SearchResults<SearchDocument> response = SourceSearchClient.Search<SearchDocument>("*", options);

            foreach (var doc in response.GetResults())
            {
                json += JsonSerializer.Serialize(doc.Document) + ",";
                json = json.Replace("\"Latitude\":", "\"type\": \"Point\", \"coordinates\": [");
                json = json.Replace("\"Longitude\":", "");
                json = json.Replace(",\"IsEmpty\":false,\"Z\":null,\"M\":null,\"CoordinateSystem\":{\"EpsgId\":4326,\"Id\":\"4326\",\"Name\":\"WGS84\"}", "]");
                json += "\n";
            }

            // Output the formatted content to a file
            json = json.Substring(0, json.Length - 3); // remove trailing comma
            File.WriteAllText(FileName, "{\"value\": [");
            File.AppendAllText(FileName, json);
            File.AppendAllText(FileName, "]}");
            Console.WriteLine("Total documents: {0}", response.TotalCount);
            json = string.Empty;
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error: {0}", ex.Message);
        }
    }

    static string GetIDFieldName()
    {
        // Find the id field of this index
        string IDFieldName = string.Empty;
        try
        {
            var schema = SourceIndexClient.GetIndex(SourceIndexName);
            foreach (var field in schema.Value.Fields)
            {
                if (field.IsKey == true)
                {
                    IDFieldName = Convert.ToString(field.Name);
                    break;
                }
            }

        }
        catch (Exception ex)
        {
            Console.WriteLine("Error: {0}", ex.Message);
        }

        return IDFieldName;
    }

    static string GetIndexSchema()
    {
        // Extract the schema for this index
        // We use REST here because we can take the response as-is
        //TODO: Perform via SearchIndexClient and/or SearchClient.

        Uri ServiceUri = new Uri($"https://{SourceSearchServiceName}.{SearchServiceDNSSuffix}");
        HttpClient HttpClient = new HttpClient();

        HttpClient.DefaultRequestHeaders.Add("api-key", SourceAdminKey);

        string Schema = string.Empty;
        try
        {
            Uri uri = new Uri(ServiceUri, "/indexes/" + SourceIndexName);
            HttpResponseMessage response = AzureSearchHelper.SendSearchRequest(HttpClient, HttpMethod.Get, uri);
            AzureSearchHelper.EnsureSuccessfulSearchResponse(response);
            Schema = response.Content.ReadAsStringAsync().Result;
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error: {0}", ex.Message);
        }

        return Schema;
    }

    private static bool DeleteIndex(string targetIndexName)
    {
        try
        {
            Console.WriteLine("\n  Delete target index {0} in {1} search service, if it exists", targetIndexName, TargetSearchServiceName);
            // Delete the index if it exists
            TargetIndexClient.DeleteIndex(targetIndexName);
        }
        catch (Exception ex)
        {
            Console.WriteLine("  Error deleting index: {0}\n", ex.Message);
            Console.WriteLine("  Did you remember to set your SearchServiceName and SearchServiceApiKey?\n");
            return false;
        }

        return true;
    }

    static void CreateTargetIndex(string sourceIndexName, string targetIndexName)
    {
        try
        {
            Console.WriteLine("\n  Create target index {0} in {1} search service", targetIndexName, TargetSearchServiceName);
            // Use the schema file to create a copy of this index
            // I like using REST here since I can just take the response as-is

            string json = File.ReadAllText(Path.Combine(BackupDirectory, sourceIndexName + ".schema"));

            // Do some cleaning of this file to change index name, etc
            json = "{" + json.Substring(json.IndexOf("\"name\""));
            int indexOfIndexName = json.IndexOf("\"", json.IndexOf("name\"") + 5) + 1;
            int indexOfEndOfIndexName = json.IndexOf("\"", indexOfIndexName);
            json = json.Substring(0, indexOfIndexName) + TargetIndexName + json.Substring(indexOfEndOfIndexName);

            Uri ServiceUri = new Uri($"https://{TargetSearchServiceName}.{SearchServiceDNSSuffix}");

            SearchIndex indexObj = JsonSerializer.Deserialize<SearchIndex>(json);

            Response<SearchIndex> response = TargetIndexClient.CreateIndex(indexObj);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error: {0}", ex.Message);
        }
    }

    static int GetCurrentDocCount(SearchClient searchClient)
    {
        // Get the current doc count of the specified index
        try
        {
            SearchOptions options = new SearchOptions
            {
                SearchMode = SearchMode.All,
                IncludeTotalCount = true
            };

            SearchResults<Dictionary<string, object>> response = searchClient.Search<Dictionary<string, object>>("*", options);
            return Convert.ToInt32(response.TotalCount);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error: {0}", ex.Message);
        }

        return -1;
    }

    static void ImportFromJSON(string sourceIndexName, string targetIndexName)
    {
        try
        {
            Console.WriteLine("\n  Upload index documents from saved JSON files");
            // Take JSON file and import this as-is to target index
            Uri ServiceUri = new Uri($"https://{TargetSearchServiceName}.{SearchServiceDNSSuffix}");
            SearchClient searchClient = TargetIndexClient.GetSearchClient(targetIndexName);

            foreach (string fileName in Directory.GetFiles(BackupDirectory, $"{sourceIndexName}*.json"))
            {
                Console.WriteLine("  -Uploading documents from file {0}", fileName);
                var docs = JsonSerializer.Deserialize<SearchDocument>(File.ReadAllText(fileName));
                var response = searchClient.UploadDocuments(docs);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error: {0}", ex.Message);
        }
    }
}
