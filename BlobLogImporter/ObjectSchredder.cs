using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;

namespace BlobLogImporter
{
    public class ObjectShredder<T>
    {
        private class DataColumnInfo
        {
            public DataColumnInfo(int ordinal)
            {
                Ordinal = ordinal;
                ContainsValue = false;
            }

            public int Ordinal { get; set; }
            public bool ContainsValue { get; set; }
        }

        private FieldInfo[] _fi;
        private PropertyInfo[] _pi;
        private Dictionary<string, DataColumnInfo> _ordinalMap;
        private Type _type;

        // ObjectShredder constructor.
        public ObjectShredder()
        {
            _type = typeof(T);
            _fi = _type.GetFields();
            _pi = _type.GetProperties();
            _ordinalMap = new Dictionary<string, DataColumnInfo>();
        }

        /// <summary>
        /// Loads a DataTable from a sequence of objects.
        /// </summary>
        /// <param name="source">The sequence of objects to load into the DataTable.</param>
        /// <param name="table">The input table. The schema of the table must match that
        /// the type T.  If the table is null, a new table is created with a schema
        /// created from the public properties and fields of the type T.</param>
        /// <param name="options">Specifies how values from the source sequence will be applied to
        /// existing rows in the table.</param>
        /// <returns>A DataTable created from the source sequence.</returns>
        public DataTable Shred(IEnumerable<T> source, DataTable table, LoadOption? options)
        {
            // Load the table from the scalar sequence if T is a primitive type.
            if (typeof(T).IsPrimitive)
            {
                return ShredPrimitive(source, table, options);
            }

            // Create a new table if the input table is null.
            if (table == null)
            {
                table = new DataTable(typeof(T).Name);
            }

            // Initialize the ordinal map and extend the table schema based on type T.
            table = ExtendTable(table, typeof(T));

            table.BeginLoadData();
            foreach (var item in source)
            {
                if (options != null)
                {
                    table.LoadDataRow(ShredObject(table, item), (LoadOption)options);
                }
                else
                {
                    table.LoadDataRow(ShredObject(table, item), true);
                }
            }
            table.EndLoadData();

            // Return the table.
            return table;
        }

        public DataTable ShredPrimitive(IEnumerable<T> source, DataTable table, LoadOption? options)
        {
            // Create a new table if the input table is null.
            if (table == null)
            {
                table = new DataTable(typeof(T).Name);
            }

            if (!table.Columns.Contains("Value"))
            {
                table.Columns.Add("Value", typeof(T));
            }

            // Enumerate the source sequence and load the scalar values into rows.
            table.BeginLoadData();
            using (IEnumerator<T> e = source.GetEnumerator())
            {
                Object[] values = new object[table.Columns.Count];
                while (e.MoveNext())
                {
                    values[table.Columns["Value"].Ordinal] = e.Current;

                    if (options != null)
                    {
                        table.LoadDataRow(values, (LoadOption)options);
                    }
                    else
                    {
                        table.LoadDataRow(values, true);
                    }
                }
            }
            table.EndLoadData();

            // Return the table.
            return table;
        }

        public object[] ShredObject(DataTable table, T instance)
        {

            FieldInfo[] fi = _fi;
            PropertyInfo[] pi = _pi;

            if (instance.GetType() != typeof(T))
            {
                // If the instance is derived from T, extend the table schema
                // and get the properties and fields.
                ExtendTable(table, instance.GetType());
                fi = instance.GetType().GetFields();
                pi = instance.GetType().GetProperties();
            }

            // Add the property and field values of the instance to an array.
            var values = new object[table.Columns.Count];
            foreach (var f in fi)
            {
                var fieldValue = f.GetValue(instance);
                var dataColumnInfo = _ordinalMap[f.Name];
                //values[dataColumnInfo.Ordinal] = fieldValue;
                if (fieldValue != null)
                {
                    values[dataColumnInfo.Ordinal] = fieldValue;
                    dataColumnInfo.ContainsValue = true;
                }
                else
                {
                    values[dataColumnInfo.Ordinal] = DBNull.Value;
                }
            }

            foreach (var p in pi)
            {
                if (_ordinalMap.ContainsKey(p.Name))
                {
                    var dataColumnInfo = _ordinalMap[p.Name];
                    var value = p.GetValue(instance, null);
                    if (value != null)
                    {
                        values[dataColumnInfo.Ordinal] = value;
                        dataColumnInfo.ContainsValue = true;
                    }
                    else
                    {
                        values[dataColumnInfo.Ordinal] = DBNull.Value;
                    }
                }
            }

            // Return the property and field values of the instance.
            return values;
        }

        public DataTable ExtendTable(DataTable table, Type type)
        {
            // Extend the table schema if the input table was null or if the value
            // in the sequence is derived from type T.
            foreach (FieldInfo f in type.GetFields())
            {
                if (!_ordinalMap.ContainsKey(f.Name))
                {
                    // Add the field as a column in the table if it doesn't exist
                    // already.

                    DataColumn dc;
                    if (table.Columns.Contains(f.Name))
                    {
                        dc = table.Columns[f.Name];
                    }
                    else
                    {
                        bool isNullable;
                        dc = table.Columns.Add(f.Name, GetInnerType(f.FieldType, out isNullable));
                        dc.AllowDBNull = isNullable;
                    }

                    // Add the field to the ordinal map.
                    _ordinalMap.Add(f.Name, new DataColumnInfo(dc.Ordinal));
                }
            }
            foreach (PropertyInfo p in type.GetProperties())
            {
                if (!_ordinalMap.ContainsKey(p.Name))
                {
                    // Add the property as a column in the table if it doesn't exist
                    // already.
                    bool isNullable;
                    var innerType = GetInnerType(p.PropertyType, out isNullable);
                    if (innerType == null)
                        continue;
                    DataColumn dc;
                    if (table.Columns.Contains(p.Name))
                    {
                        dc = table.Columns[p.Name];
                    }
                    else
                    {
                        dc = table.Columns.Add(p.Name, innerType);
                        dc.AllowDBNull = isNullable;
                    }

                    // Add the property to the ordinal map.
                    _ordinalMap.Add(p.Name, new DataColumnInfo(dc.Ordinal));
                }
            }

            // Return the table.
            return table;
        }

        public Type GetInnerType(Type type, out bool isNullable)
        {
            if (type.IsGenericType)
            {
                if (type.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    isNullable = true;
                    return Nullable.GetUnderlyingType(type);
                }
            }
            else if (type == typeof(String))
            {
                isNullable = true;
                return type;
            }
            else if (type.IsValueType && !string.IsNullOrEmpty(type.Namespace) && type.Namespace.ToLower() == "system")
            {
                isNullable = false;
                return type;
            }
            isNullable = true;
            return null;
        }

        public void AddColumnMappings(SqlBulkCopyColumnMappingCollection columnMappings)
        {
            if (_ordinalMap == null || !_ordinalMap.Any())
                throw new Exception("Shred collection before trying to add columnMappings");

            foreach (var mapping in _ordinalMap)
            {
                if (mapping.Value.ContainsValue)
                    columnMappings.Add(mapping.Key, mapping.Key);
            }
        }
    }
}