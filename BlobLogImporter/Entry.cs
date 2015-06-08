using System;
using System.ComponentModel.DataAnnotations;

namespace BlobLogImporter
{
    public class Entry
    {
        [Key]
        public int Id { get; set; }

        public string VidyanoMethod { get; set; }

        public DateTimeOffset CreatedOn { get; set; }

        public Guid? UserId { get; set; }

        public Guid? QueryId { get; set; }

        public string ParentId { get; set; }

        public string ParentObjectId { get; set; }

        public string Action { get; set; }

        public string Notification { get; set; }

        public string ReportId { get; set; }

        public string ReportFormat { get; set; }

        public string Uri { get; set; }

        public string HttpMethod { get; set; }

        public short ResponseCode { get; set; }

        public string UserHostAddress { get; set; }

        public string VidyanoVersion { get; set; }

        public long OutgoingDataLength { get; set; }

        public Guid OutgoingDataId { get; set; }

        public int IncomingDataLength { get; set; }

        public long EllapsedMilliseconds { get; set; }

        public Guid IncomingDataId { get; set; }

        public string TextSearch { get; set; }
    }
}