using System.Diagnostics;
using System.Reflection;
using System.Xml.Linq;
using Microsoft.Data.SqlClient;

namespace BlobLogImporter;

internal class Program
{
    private static async Task<int> Main(string[] args)
    {
        var assembly = Assembly.GetExecutingAssembly();
        Console.WriteLine($"BlobLogImporter v{assembly.GetName().Version?.Major}.{assembly.GetName().Version?.Minor} (c) Steve Hansen 2015");

        if (args.Length != 6)
        {
            var self = Path.GetFileName(assembly.Location);
            Console.WriteLine($" Usage: {self} server database user password table path");
            return 1;
        }

        // USE master
        // CREATE LOGIN {user} WITH PASSWORD = '{password}'
        // USE {database}
        // CREATE USER {user} FOR LOGIN {user} WITH DEFAULT_SCHEMA = dbo
        // GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE ON [dbo].[{table}] to {user}

        try
        {
            await RunAsync(args[0], args[1], args[2], args[3], args[4], args[5]);
            return 0;
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(ex.ToString());
            Console.ResetColor();

            Console.WriteLine("Press any key to continue...");
            Console.ReadKey();
            return -1;
        }
    }

    private static async Task RunAsync(string server, string database, string user, string password, string table, string path)
    {
        if (!File.Exists(path))
            throw new ArgumentException("File does not exist.", nameof(path));

        var feed = await LoadXmlAsync(path);
        var entries = CreateEntries(feed);

        var sw = Stopwatch.StartNew();
        Console.Write("Inserting ");
        Console.Write(entries.Count);
        Console.WriteLine(" entries...");

        await BulkInsertAsync(entries, $"Data Source={server};Initial Catalog={database};Persist Security Info=True;User ID={user};Password={password};Application Name=Logging Importer;TrustServerCertificate=True", table);
        Console.WriteLine(sw.Elapsed);
    }

    private static async Task<XElement> LoadXmlAsync(string path)
    {
        await using var stream = File.OpenRead(path);
        return await XElement.LoadAsync(stream, LoadOptions.None, CancellationToken.None);
    }

    private static List<Entry> CreateEntries(XElement feed)
    {
        var entries = new List<Entry>();

        XNamespace ns = "http://www.w3.org/2005/Atom";
        XNamespace d = "http://schemas.microsoft.com/ado/2007/08/dataservices";
        XNamespace m = "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata";

        foreach (var entry in feed.Elements(ns + "entry"))
        {
            var properties = entry.Element(ns + "content")?.Element(m + "properties");
            if (properties == null)
                continue;

            var dbEntry = new Entry
            {
                CreatedOn = (DateTimeOffset?)properties.Element(d + "Timestamp") ?? default,
                HttpMethod = (string?)properties.Element(d + "HttpMethod"),
                Uri = (string?)properties.Element(d + "Uri"),
                VidyanoVersion = (string?)properties.Element(d + "VidyanoVersion"),
                UserHostAddress = (string?)properties.Element(d + "UserHostAddress"),
                VidyanoMethod = (string?)properties.Element(d + "VidyanoMethod"),
                ParentId = (string?)properties.Element(d + "ParentId"),
                ParentObjectId = (string?)properties.Element(d + "ParentObjectId"),
                Action = (string?)properties.Element(d + "Action"),
                Notification = (string?)properties.Element(d + "Notification"),
                ReportId = (string?)properties.Element(d + "ReportId"),
                ReportFormat = (string?)properties.Element(d + "ReportFormat"),
                // ReSharper disable once StringLiteralTypo
                EllapsedMilliseconds = (long?)properties.Element(d + "EllapsedMilliseconds") ?? 0,
                OutgoingDataLength = (long?)properties.Element(d + "OutgoingDataLength") ?? 0,
                IncomingDataLength = (int?)properties.Element(d + "OutgoingDataLength") ?? 0,
                ResponseCode = (short?)properties.Element(d + "ResponseCode") ?? 0,
                UserId = (Guid?)properties.Element(d + "UserId"),
                QueryId = (Guid?)properties.Element(d + "QueryId"),
                TextSearch = (string?)properties.Element(d + "TextSearch"),
            };

            var incomingDataId = properties.Element(d + "IncomingDataId");
            dbEntry.IncomingDataId = incomingDataId == null ? Guid.Empty : (Guid)incomingDataId;

            var outgoingDataId = properties.Element(d + "OutgoingDataId");
            dbEntry.OutgoingDataId = outgoingDataId == null ? Guid.Empty : (Guid)outgoingDataId;

            entries.Add(dbEntry);
        }

        return entries;
    }

    private static async Task BulkInsertAsync<T>(IEnumerable<T> objects, string bulkInsertConnectionString, string destinationTableName)
    {
        await using var connection = new SqlConnection(bulkInsertConnectionString);
        await connection.OpenAsync();

        using var bulk = new SqlBulkCopy(connection);

        bulk.NotifyAfter = 10000;
        bulk.BulkCopyTimeout = 600;
        bulk.DestinationTableName = destinationTableName;

        bulk.SqlRowsCopied += (_, args) => Console.WriteLine($" {args.RowsCopied}");

        var objectShredder = new ObjectShredder<T>();
        var dataTable = objectShredder.Shred(objects, null, null);
        objectShredder.AddColumnMappings(bulk.ColumnMappings);

        await bulk.WriteToServerAsync(dataTable);
    }
}