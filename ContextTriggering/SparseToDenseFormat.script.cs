using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;
using System.Linq;



public class ToDenseProcessor : Processor
{
    public override Schema Produces(string[] columns, string[] args, Schema input)
    {
        Schema s = new Schema();
        s.Add(new ColumnInfo("Instance", ColumnDataType.String));
        return s;
    }

    public override IEnumerable<Row> Process(RowSet input, Row output, string[] args)
    {
        foreach (var row in input.Rows)
        {
            var sparseStr = row["Instance"].String;
            if (String.IsNullOrEmpty(sparseStr) || String.IsNullOrWhiteSpace(sparseStr))
                continue;
            var strs = sparseStr.Split('\t').ToList();
            var outputStrs = new List<string>();

            for (int i = 0; i < 3; i++)
            {
                outputStrs.Add(strs[i]);
            }
            var dicFeats = new Dictionary<int, double>();
            var featureNum = int.Parse(strs[3]);
            for (int i = 0; i < featureNum; i++)
            {
                dicFeats.Add(i, 0.0);
            }

       
            for (int i = 4; i < strs.Count(); i++)               
            {
                var sp = strs[i].Split(':');
                dicFeats[int.Parse(sp[0])] = double.Parse(sp[1]);
            }
            for (int i = 0; i < featureNum; i++)
                outputStrs.Add(dicFeats[i].ToString());

            output["Instance"].Set(String.Join("\t", outputStrs));
            yield return output;
        }
    }
}