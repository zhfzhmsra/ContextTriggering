using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;
using System.Diagnostics;
using System.Linq;


public class CountReducer : Reducer
{
    public override Schema Produces(string[] requested_columns, string[] args, Schema input_schema)
    {
        var outputSchema = new Schema();

        outputSchema.Add(new ColumnInfo("Threshold", typeof(string)));
        outputSchema.Add(new ColumnInfo("PosPos", typeof(long)));
        outputSchema.Add(new ColumnInfo("PosNeg", typeof(long)));
        outputSchema.Add(new ColumnInfo("NegPos", typeof(long)));
        outputSchema.Add(new ColumnInfo("NegNeg", typeof(long)));

        return outputSchema;
    }

    public override IEnumerable<Row> Reduce(RowSet input, Row output, string[] args)
    {
        //double threshold=Convert.ToDouble(args[0]);


        var rows = new List<Row>();

        foreach (Row row in input.Rows)
        {
            rows.Add(row.Clone());
        }

        for (double threshold = 0.05; threshold < 1; threshold += 0.05)
        {
            long pospos = 0;
            long negpos = 0;
            long posneg = 0;
            long negneg = 0;
            foreach (Row row in rows)
            {
                int label = row["Rating"].Integer;
                double score = double.Parse(row["Score"].String);
                if (score < threshold)
                {
                    if (label == 1) negpos++;
                    else negneg++;
                }
                else
                {
                    if (label == 1) pospos++;
                    else posneg++;
                }
            }     
            output["Threshold"].Set(threshold.ToString("0.00"));
            output["PosPos"].Set(pospos);
            output["PosNeg"].Set(posneg);
            output["NegPos"].Set(negpos);
            output["NegNeg"].Set(negneg);

            yield return output;
        }
    }
}

public class MyEvaluate : Reducer
{
    public override Schema Produces(string[] requested_columns, string[] args, Schema input_schema)
    {
        var outputSchema = new Schema();

        outputSchema.Add(new ColumnInfo("Threshold", typeof(string)));
        outputSchema.Add(new ColumnInfo("Precision", typeof(double)));
        outputSchema.Add(new ColumnInfo("Recall", typeof(double)));
        outputSchema.Add(new ColumnInfo("F1score", typeof(double)));
        outputSchema.Add(new ColumnInfo("Accuracy", typeof(double)));

        return outputSchema;
    }

    class EvalObject
    {
        public string Threshold;
        public long PosPos;
        public long PosNeg;
        public long NegPos;
        public long NegNeg;
    }

    public override IEnumerable<Row> Reduce(RowSet input, Row output, string[] args)
    {
        //double threshold=Convert.ToDouble(args[0]);


        var rs = new List<EvalObject>();

        foreach (Row row in input.Rows)
        {
            rs.Add(
                new EvalObject()
                {
                    Threshold = row["Threshold"].String,
                    PosPos = row["PosPos"].Long,
                    PosNeg = row["PosNeg"].Long,
                    NegPos = row["NegPos"].Long,
                    NegNeg = row["NegNeg"].Long

                }
                ); 
        }

        var dic = rs.GroupBy(dp => dp.Threshold).ToDictionary(dp => dp.Key, dp => dp.ToList());
        foreach (var pair in dic)
        {
            var pospos = pair.Value.Select(dp => dp.PosPos).Sum();
            var posneg = pair.Value.Select(dp => dp.PosNeg).Sum();
            var negpos = pair.Value.Select(dp => dp.NegPos).Sum();
            var negneg = pair.Value.Select(dp => dp.NegNeg).Sum();

            double precision = ((double)(pospos)) / (pospos + posneg);

            double recall = ((double)(pospos)) / (pospos + negpos);

            double fscore = 2 * precision * recall / (precision + recall);

            double accuracy = ((double)(pospos + negneg)) / (
                pospos + posneg + negpos + negneg
                );

            output["Threshold"].Set(pair.Key);
            output["Precision"].Set(precision);
            output["Recall"].Set(recall);
            output["F1score"].Set(fscore);
            output["Accuracy"].Set(accuracy);

            yield return output;
        }                   
    }
}
