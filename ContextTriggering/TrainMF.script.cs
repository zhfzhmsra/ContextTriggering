using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;
using System.Linq;




public class VectorNullProcessor : Processor
{
    public override Schema Produces(string[] requestedColumns, string[] args, Schema inputSchema)
    {
        return inputSchema.Clone();
    }

    public override IEnumerable<Row> Process(RowSet input, Row outputRow, string[] args)
    {
        var K = int.Parse(args[0]);

        foreach (Row row in input.Rows)
        {
            var vector = row["Vector"].String;
            if (String.IsNullOrEmpty(vector))
            {
                vector = string.Join(",", Enumerable.Range(0, K).Select(dp => "0"));
            }
            outputRow[0].Set(row[0].String);
            outputRow[1].Set(vector);
            yield return outputRow;
        }
    }
}