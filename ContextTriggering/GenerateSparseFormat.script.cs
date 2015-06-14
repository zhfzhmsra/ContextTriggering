using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;


public class Processor_BaseFeatureSparse : Processor
{
    static HashSet<string> metaColumns = new HashSet<string>(new string[] { "User_Anid", "Contexts", "Rating" });
    public override Schema Produces(string[] columns, string[] args, Schema input)
    {
        Schema s = new Schema();

        foreach (var name in metaColumns)
        {
            if (name == "Rating")
                s.Add(new ColumnInfo(name, ColumnDataType.Integer));
            else
                s.Add(new ColumnInfo(name, ColumnDataType.String));
        }
        s.Add(new ColumnInfo("FeatureNum", ColumnDataType.Integer));
        s.Add(new ColumnInfo("SparseFeatures", ColumnDataType.String));
        return s;
    }
    public override IEnumerable<Row> Process(RowSet input, Row output, string[] args)
    {
        foreach (var row in input.Rows)
        {
            int i = 0;
            var outputStrs = new List<string>();
            foreach (var col in row.Schema.Columns)
            {
                if (metaColumns.Contains(col.Name))
                {
                    output[col.Name].Set(row[col.Name].Value);
                }                
                else
                {
                    if (row[col.Name].IsNull() || row[col.Name].Float == 0)
                    {
                    }
                    else
                    {
                        outputStrs.Add((i) + ":" + row[col.Name].Float);
                    }
                    i++;
                }
            }
            output["FeatureNum"].Set(row.Columns.Count() - metaColumns.Count());
            output["SparseFeatures"].Set(String.Join("\t", outputStrs));
            yield return output;
        }
    }
}