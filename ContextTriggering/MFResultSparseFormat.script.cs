using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;



public class Processor_FeatureSparse : Processor
{
   
    public override Schema Produces(string[] columns, string[] args, Schema input)
    {
        Schema s = new Schema();
        s.Add(new ColumnInfo(args[0], ColumnDataType.String));
        s.Add(new ColumnInfo("Add_FeatureNum", ColumnDataType.Integer));
        s.Add(new ColumnInfo("Add_FeatureVector", ColumnDataType.String));
        return s;
    }
    public override IEnumerable<Row> Process(RowSet input, Row output, string[] args)
    {
        foreach (var row in input.Rows)
        {
            output[args[0]].Set(row[args[0]].String);
            var strs = row["Vector"].String.Split(',');

            var featureStrs = new List<string>();
            for (int i = 0; i < strs.Length; i++)
            {
                if (float.Parse(strs[i]) != 0)
                    featureStrs.Add(i.ToString() + ":" + strs[i]);
            }
            output["Add_FeatureNum"].Set(strs.Length);
            output["Add_FeatureVector"].Set(String.Join("\t", featureStrs));
            yield return output;
        }
    }
}