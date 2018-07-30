using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ArtGalleryIndexer
{
    class Program
    {
        private static string searchServiceName = "[SearchServiceName]";
        private static string apiKey = "[SearchServiceApiKey]";
        private static SearchServiceClient _searchClient;
        private static ISearchIndexClient _indexClient;
        private static string AzureSearchIndex = "[indexName]";

        static void Main(string[] args)
        {
            _searchClient = new SearchServiceClient(searchServiceName, new SearchCredentials(apiKey));
            _indexClient = _searchClient.Indexes.GetClient(AzureSearchIndex);

            Console.WriteLine("{0}", "Deleting index...\n");
            DeleteIndex();

            Console.WriteLine("{0}", "Creating index...\n");
            if (CreateIndex() == false)
            {
                Console.ReadLine();
                return;
            }
        }

        private static bool DeleteIndex()
        {
            try
            {
                _searchClient.Indexes.Delete(AzureSearchIndex);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error deleting index: {0}\r\n", ex.Message);
                Console.WriteLine("Did you remember to set your searchServiceName and searchServiceApiKey?\r\n");
                return false;
            }
            return true;
        }

        private static bool CreateIndex()
        {
            CorsOptions co = new CorsOptions();
            List<string> origins = new List<string>();
            origins.Add("*");
            co.AllowedOrigins = origins;
            try
            {
                var Definition = new Index()
                {
                    Name = AzureSearchIndex,
                    CorsOptions = co,
                    Fields = new[]
                    {
                        new Field("acno", DataType.String)                                              { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("acquisitionYear", DataType.Int32)                                    { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("all_artists", DataType.String)                                       { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("catalogueGroupCompleteStatus", DataType.String)                      { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("catalogueGroupFinbergNumber", DataType.String)                       { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("catalogueGroupGroupType", DataType.String)                           { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("catalogueGroupId", DataType.Int32)                                   { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("catalogueGroupShortTitle", DataType.String)                          { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("classification", DataType.String)                                    { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("contributorCount", DataType.Int32)                                   { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("contributors", DataType.Collection(DataType.String))                 { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("creditLine", DataType.String)                                        { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("dateRange", DataType.String)                                         { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("dateText", DataType.String)                                          { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("depth", DataType.String)                                             { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("dimensions", DataType.String)                                        { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("foreignTitle", DataType.String)                                      { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("groupTitle", DataType.String)                                        { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("height", DataType.String)                                            { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("width", DataType.String)                                             { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("id", DataType.Int32)                                                 { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("inscription", DataType.String)                                       { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("medium", DataType.String)                                            { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("movementCount", DataType.Int32)                                      { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("subjectCount", DataType.Int32)                                       { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("subjects", DataType.Collection(DataType.String))                     { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("thumbnailCopyright", DataType.String)                                { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("thumbnailUrl", DataType.String)                                      { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("title", DataType.String)                                             { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("units", DataType.String)                                             { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },
                        new Field("url", DataType.String)                                               { IsKey = true, IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false }
                    }
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error creating index: {0}\r\n", ex.Message);
                return false;
            }
            return true;
        }

        private static void UploadContent()
        {
            var indexOperations = new List<IndexAction>();
            string[] files = Directory.GetFiles(@"", "*.json", System.IO.SearchOption.AllDirectories);
            int totalCounter = 0;
            try
            {
                foreach (var file in files)
                {
                    using (StreamReader jsonFile = File.OpenText(file))
                    {
                        totalCounter++;
                        Document document = new Document();
                        string json = jsonFile.ReadToEnd();
                        dynamic array = JsonConvert.DeserializeObject(json);

                        document.Add("acno", array["acno"].Value);
                        document.Add("acquisitionYear", array["acquisitionYear"] == null  ? -1 : array["acquisitionYear"].Value);
                        document.Add("all_artists", array["all_artists"].Value);
                        document.Add("catalogueGroupCompleteStatus", array["catalogueGroupCompleteStatus"] == null ? "" : array["catalogueGroupCompleteStatus"].Value);
                        document.Add("catalogueGroupFinbergNumber", array["catalogueGroupFinbergNumber"] == null ? "" : array["catalogueGroupFinbergNumber"].Value);
                        document.Add("catalogueGroupGroupType", array["catalogueGroupGroupType"] == null ? "" : array["catalogueGroupGroupType"].Value);
                        document.Add("catalogueGroupId", array["catalogueGroupGroupType"] == null ? -1 : array["catalogueGroupGroupType"].Value);
                        document.Add("catalogueGroupShortTitle", array["catalogueGroupShortTitle"] == null ? "" : array["catalogueGroupShortTitle"].Value);
                        document.Add("classification", array["classification"] == null ? "" : array["classification"].Value);

                        document.Add("contributorCount", array["contributorCount"] == null ? -1 : array["contributorCount"].Value);
                        document.Add("creditLine", array["creditLine"].Value);
                        document.Add("dateRange", array["dateRange"].Value);
                        document.Add("dateText", array["dateText"].Value);
                        document.Add("depth", array["depth"] == null ? "" : array["depth"].Value);
                        document.Add("width", array["width"] == null ? "" : array["width"].Value);
                        document.Add("height", array["height"] ==null ? "" : array["height"].Value);
                        document.Add("dimensions", array["dimensions"].Value);
                        document.Add("foreignTitle", array["foreignTitle"].Value);
                        document.Add("groupTitle", array["groupTitle"].Value);
                        document.Add("id", array["id"] == null ? -1 : array["id"].Value);
                        document.Add("inscription", array["inscription"].Value);
                        document.Add("medium", array["medium"].Value);
                        document.Add("movementCount", array == null ? -1 : array["movementCount"].Value);
                        document.Add("subjectCount", array["subjectCount"].Value == null ? -1 : array["subjectCount"].Value);
                        document.Add("thumbnailCopyright", array["thumbnailCopyright"] == null ? -1 : array["thumbnailCopyright"].Value);
                        document.Add("thumbnailUrl", array["thumbnailUrl"].Value);
                        document.Add("title", array["title"].Value);
                        document.Add("units", array["units"].Value);
                        document.Add("url", array["url"].Value);

                        if (array["contributers"] != null)
                        {
                            List<string> contributerList = new List<string>();
                            foreach (var item in array["contributers"])
                            {
                                contributerList.Add(((string)item["fc"]));
                            }
                            document.Add("contributers", contributerList);
                        }

                        if (array["subjects"] != null)
                        {
                            JArray subArray = array["subjects"]["children"];
                            List<string> subjectList = new List<string>();
                            if (subArray != null)
                            {
                                foreach (var item in subArray.Children())
                                {
                                    var itemProperties = item.Children<JProperty>();
                                    var myChildren = itemProperties.FirstOrDefault(x => x.Name == "children");
                                    foreach (var subItem in myChildren.Children())
                                    {
                                        foreach (var secondLevelChild in subItem)
                                        {
                                            foreach (var thirdLevelChild in secondLevelChild["children"])
                                            {
                                                var name = thirdLevelChild["name"];
                                                subjectList.Add(name.ToString());
                                            }
                                            var secondLevelItemProperties = secondLevelChild.Children<JProperty>();
                                            var secondLevelElement = secondLevelItemProperties.FirstOrDefault(x => x.Name == "name");
                                            subjectList.Add(secondLevelElement.Value.ToString());
                                        }
                                    }
                                    var rootElement = itemProperties.FirstOrDefault(x => x.Name == "name");
                                    subjectList.Add(rootElement.Value.ToString());
                                }
                            }
                            document.Add("subjects", subjectList);
                        }
                        indexOperations.Add(IndexAction.Upload(document));
                        if (indexOperations.Count >= 500)
                        {
                            Console.WriteLine("Writing {0} documents of {1} total documents...", indexOperations.Count, totalCounter);
                            _indexClient.Documents.Index(new IndexBatch(indexOperations));
                            indexOperations.Clear();
                        }
                    }
                }
                if (indexOperations.Count >= 0)
                {
                    Console.WriteLine("Writing {0} documents of {1} total documents...", indexOperations, totalCounter);
                    _indexClient.Documents.Index(new IndexBatch(indexOperations));
                }
            }
            catch (IndexBatchException e)
            {
                Console.WriteLine("Failed to index some of the documents: {0}", String.Join
                    (", ", e.IndexingResults.Where(r => !r.Succeeded).Select(r => r.Key)));
            }
        }
    }
}
