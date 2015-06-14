using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;



public class Processor_AddSparseFeatures : Processor
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

            foreach (var name in metaColumns)
            {
                output[name].Set(row[name].Value);
            }
            output["FeatureNum"].Set(row["FeatureNum"].Integer + row["Add_FeatureNum"].Integer);

            var addFeatureStr = "";
            if (!row["Add_FeatureVector"].IsNull() && row["Add_FeatureVector"].String.Length > 0)
                addFeatureStr = "\t" + String.Join("\t", row["Add_FeatureVector"].String.Split('\t')
                 .Select(dp => (int.Parse(dp.Split(':')[0]) + row["FeatureNum"].Integer) + ":" + dp.Split(':')[1]));
            output["SparseFeatures"].Set(row["SparseFeatures"].String + addFeatureStr);

            yield return output;
        }
    }
}