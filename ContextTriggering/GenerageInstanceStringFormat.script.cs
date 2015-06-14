
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;


public class Processor_InstanceFormat : Processor
{
    public override Schema Produces(string[] columns, string[] args, Schema input)
    {
        Schema s = new Schema();
        s.Add(new ColumnInfo("Instance", ColumnDataType.String));
        return s;
    }

    public override IEnumerable<Row> Process(RowSet input, Row output, string[] args)
    {

        var labelStr = args[0];
        var userStr = args[1];
        var itemStr = args[2];

        foreach (var row in input.Rows)
        {
            try

            {
                if (String.IsNullOrEmpty(row["SparseFeatures"].String))
                    continue;
               
                output["Instance"].Set(row[labelStr].Integer.ToString() + "\t" + row[userStr].String
                    + "\t" + row[itemStr].String + "\t" + row["FeatureNum"].Integer.ToString()
                    + "\t" + row["SparseFeatures"].String
                    );
            }
            catch (Exception e)
            {
                continue;
            }
            yield return output;
        }
    }