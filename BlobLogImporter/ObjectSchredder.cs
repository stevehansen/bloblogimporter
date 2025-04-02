using System.Data;
using System.Reflection;
using Microsoft.Data.SqlClient;

namespace BlobLogImporter;

public sealed class ObjectShredder<T>
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

    private readonly FieldInfo[] _fi;
    private readonly PropertyInfo[] _pi;
    private readonly Dictionary<string, DataColumnInfo> _ordinalMap;

    // ObjectShredder constructor.
    public ObjectShredder()
    {
        var type = typeof(T);
        _fi = type.GetFields();
        _pi = type.GetProperties();
        _ordinalMap = new();
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
    public DataTable Shred(IEnumerable<T> source, DataTable? table, LoadOption? options)
    {
        // Load the table from the scalar sequence if T is a primitive type.
        if (typeof(T).IsPrimitive)
        {
            return ShredPrimitive(source, table, options);
        }

        // Create a new table if the input table is null.
        table ??= new(typeof(T).Name);

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

    public DataTable ShredPrimitive(IEnumerable<T> source, DataTable? table, LoadOption? options)
    {
        // Create a new table if the input table is null.
        table ??= new(typeof(T).Name);

        if (!table.Columns.Contains("Value"))
        {
            table.Columns.Add("Value", typeof(T));
        }

        // Enumerate the source sequence and load the scalar values into rows.
        table.BeginLoadData();
        using (IEnumerator<T> e = source.GetEnumerator())
        {
            object[] values = new object[table.Columns.Count];
            while (e.MoveNext())
            {
                values[table.Columns["Value"].Ordinal] = e.Current!;

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

        if (instance!.GetType() != typeof(T))
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
            if (_ordinalMap.TryGetValue(p.Name, out var dataColumnInfo))
            {
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
                    var isNullable = GetInnerType(f.FieldType, out var innerType);
                    dc = table.Columns.Add(f.Name, innerType!);
                    dc.AllowDBNull = isNullable;
                }

                // Add the field to the ordinal map.
                _ordinalMap.Add(f.Name, new(dc.Ordinal));
            }
        }
        foreach (PropertyInfo p in type.GetProperties())
        {
            if (!_ordinalMap.ContainsKey(p.Name))
            {
                // Add the property as a column in the table if it doesn't exist
                // already.
                var isNullable = GetInnerType(p.PropertyType, out var innerType);
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
                _ordinalMap.Add(p.Name, new(dc.Ordinal));
            }
        }

        // Return the table.
        return table;
    }

    public bool GetInnerType(Type type, out Type? innerType)
    {
        bool isNullable = false;

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
        {
            isNullable = true;
            innerType = Nullable.GetUnderlyingType(type);
            return isNullable;
        }

        if (type == typeof(string))
        {
            isNullable = true;
            innerType = type;
            return isNullable;
        }

        if (type.IsValueType && !string.IsNullOrEmpty(type.Namespace) &&
            type.Namespace.ToLower() == "system")
        {
            isNullable = false;
            innerType = type;
            return isNullable;
        }

        isNullable = true;
        innerType = null;
        return isNullable;
    }

    public void AddColumnMappings(SqlBulkCopyColumnMappingCollection columnMappings)
    {
        if (_ordinalMap == null || !_ordinalMap.Any())
            throw new("Shred collection before trying to add columnMappings");

        foreach (var mapping in _ordinalMap)
        {
            if (mapping.Value.ContainsValue)
                columnMappings.Add(mapping.Key, mapping.Key);
        }
    }
}