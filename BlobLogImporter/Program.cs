using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Xml.Linq;

namespace BlobLogImporter
{
    class Program
    {
        static void Main(string[] args)
        {
            var assembly = typeof(Program).Assembly;
            Console.WriteLine("BlobLogImporter v{0}.{1} (c) Steve Hansen 2015", assembly.GetName().Version.Major, assembly.GetName().Version.Minor);
            if (args.Length != 6)
            {
                var self = Path.GetFileName(assembly.Location);
                Console.WriteLine(" Usage: {0} server database user password table path", self);
                return;
            }

            // USE master
            // CREATE LOGIN {user} WITH PASSWORD = '{password}'
            // USE {database}
            // CREATE USER {user} FOR LOGIN {user} WITH DEFAULT_SCHEMA = dbo
            // GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE ON [dbo].[{table}] to {user}

            try
            {
                Run(args[0], args[1], args[2], args[3], args[4], args[5]);
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.ToString());
                Console.ResetColor();

                Console.WriteLine("Press any key to continue...");
                Console.ReadKey();
            }
        }

        static void Run(string server, string database, string user, string password, string table, string path)
        {
            if (!File.Exists(path))
                throw new ArgumentException("File does not exist.", nameof(path));

            var feed = XElement.Load(path);

            var entries = CreateEntries(feed);

            var sw = Stopwatch.StartNew();
            Console.Write("Inserting ");
            Console.Write(entries.Count);
            Console.WriteLine(" entries...");

            BulkInsert(entries, $"Data Source={server};Initial Catalog={database};Persist Security Info=True;User ID={user};Password={password};Application Name=Logging Importer", table);
            Console.WriteLine(sw.Elapsed);
        }

        static List<Entry> CreateEntries(XElement feed)
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

                var dbEntry = new Entry();
                dbEntry.CreatedOn = (DateTimeOffset)properties.Element(d + "Timestamp");
                dbEntry.HttpMethod = (string)properties.Element(d + "HttpMethod");
                dbEntry.Uri = (string)properties.Element(d + "Uri");
                dbEntry.VidyanoVersion = (string)properties.Element(d + "VidyanoVersion");
                dbEntry.UserHostAddress = (string)properties.Element(d + "UserHostAddress");
                dbEntry.VidyanoMethod = (string)properties.Element(d + "VidyanoMethod");
                dbEntry.ParentId = (string)properties.Element(d + "ParentId");
                dbEntry.ParentObjectId = (string)properties.Element(d + "ParentObjectId");
                dbEntry.Action = (string)properties.Element(d + "Action");
                dbEntry.Notification = (string)properties.Element(d + "Notification");
                dbEntry.EllapsedMilliseconds = (long)properties.Element(d + "EllapsedMilliseconds");
                dbEntry.OutgoingDataLength = (long)properties.Element(d + "OutgoingDataLength");
                dbEntry.IncomingDataLength = (int)properties.Element(d + "OutgoingDataLength");
                dbEntry.ResponseCode = (short)properties.Element(d + "ResponseCode");
                dbEntry.UserId = (Guid?)properties.Element(d + "UserId");
                dbEntry.QueryId = (Guid?)properties.Element(d + "QueryId");

                var incomingDataId = properties.Element(d + "IncomingDataId");
                dbEntry.IncomingDataId = incomingDataId == null ? Guid.Empty : (Guid)incomingDataId;
                var outgoingDataId = properties.Element(d + "OutgoingDataId");
                dbEntry.OutgoingDataId = outgoingDataId == null ? Guid.Empty : (Guid)outgoingDataId;

                entries.Add(dbEntry);
            }

            return entries;
        }

        static void BulkInsert<T>(IEnumerable<T> objects, string bulkInsertConnectionString, string destinationTableName)
        {
            using (var bulk = new SqlBulkCopy(bulkInsertConnectionString, SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.CheckConstraints))
            {
                bulk.NotifyAfter = 10000;
                bulk.BulkCopyTimeout = 600;
                bulk.DestinationTableName = destinationTableName;
                bulk.SqlRowsCopied += (sender, args) => Console.WriteLine(" " + args.RowsCopied);

                var objectShredder = new ObjectShredder<T>();

                var dataTable = objectShredder.Shred(objects, null, null);

                objectShredder.AddColumnMappings(bulk.ColumnMappings);

                bulk.WriteToServer(dataTable);
            }
        }
    }
}