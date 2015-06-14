using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;
using System.Linq;


public class Util
{
    public static void Shuffle<T>(ref List<T> list, Random rng)
    {
        int n = list.Count;
        while (n > 1)
        {
            n--;
            int k = rng.Next(n + 1);
            T value = list[k];
            list[k] = list[n];
            list[n] = value;
        }
    }
}


public class BalanceReducer : Reducer
{
    public override Schema Produces(string[] columns, string[] args, Schema input)
    {
        return input.Clone();
    }

    public override IEnumerable<Row> Reduce(RowSet input, Row output, string[] args)
    {
        var reduceKey = args[0];
        var label = args[1];
        Dictionary<string, List<Row>> dicRows = new Dictionary<string, List<Row>>();
        foreach (var row in input.Rows)
        {
            var key = row[reduceKey].String;
            if (!dicRows.ContainsKey(key))
                dicRows.Add(key, new List<Row>());
            dicRows[key].Add(row);
        }

        foreach (var pair in dicRows)
        {
            var pos = pair.Value.Where(row => row[label].Integer == 1).ToList();
            var negs = pair.Value.Where(row => row[label].Integer == 0).ToList();

            var outputs = new List<Row>();
            outputs.AddRange(pos);

            if (pos.Count > negs.Count)
                outputs.AddRange(negs);
            else
            {
                Util.Shuffle<Row>(ref negs, new Random());
                for (int i = 0; i < pos.Count; i++)
                    outputs.Add(negs[i]);
            }

            foreach (var o in outputs)
            {
                output = o.Clone();
                yield return output;
            }
        }
    }
}




public class BalanceProcessorPlus : Processor
{
    public override Schema Produces(string[] columns, string[] args, Schema input)
    {
        return input.Clone();
    }

    public override IEnumerable<Row> Process(RowSet input, Row output, string[] args)
    {
        var reduceKey = args[0];
        var label = args[1];
        var pos = new List<Row>();
        var neg = new List<Row>();
        foreach (var row in input.Rows)
        {
            if (row[label].Integer > 0)
                pos.Add(row.Clone());
            else
                neg.Add(row.Clone());
        }

        var res = new List<Row>();
        res.AddRange(pos);
        res.AddRange(neg);
        if (pos.Any() && pos.Count < neg.Count)
        {
            var addCount = neg.Count - pos.Count;
            int times = addCount / pos.Count;
            for (int i = 0; i < times; i++)
                res.AddRange(pos);
            addCount = addCount % pos.Count;
            Util.Shuffle<Row>(ref pos, new Random());
            res.AddRange(pos.Take(addCount));
        }
        else if (neg.Any() && neg.Count < pos.Count)
        {
            var addCount = pos.Count - neg.Count;
            int times = addCount / neg.Count;
            for (int i = 0; i < times; i++)
                res.AddRange(neg);
            addCount = addCount % neg.Count;
            Util.Shuffle<Row>(ref neg, new Random());
            res.AddRange(neg.Take(addCount));
        }

        foreach (var o in res)
        {
            output = o.Clone();
            yield return output;
        }
    }
}


public class BalanceProcessorReduce : Processor
{
    public override Schema Produces(string[] columns, string[] args, Schema input)
    {
        return input.Clone();
    }

    public override IEnumerable<Row> Process(RowSet input, Row output, string[] args)
    {
        var reduceKey = args[0];
        var label = args[1];
        var pos = new List<Row>();
        var neg = new List<Row>();
        foreach (var row in input.Rows)
        {
            if (row[label].Integer > 0)
                pos.Add(row.Clone());
            else
                neg.Add(row.Clone());
        }

        var res = new List<Row>();
        if (pos.Any() && pos.Count < neg.Count)
        {
            res.AddRange(pos);
            Util.Shuffle<Row>(ref neg);
            res.AddRange(neg.Take(pos.Count));
        }
        else if (neg.Any() && neg.Count < pos.Count)
        {
            res.AddRange(neg);
            Util.Shuffle<Row>(ref pos);
            res.AddRange(pos.Take(neg.Count));
        }
        Util.Shuffle<Row>(ref res);
        foreach (var o in res)
        {
            output = o.Clone();
            yield return output;
        }
    }
}