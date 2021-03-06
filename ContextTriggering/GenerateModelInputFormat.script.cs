﻿
using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using ScopeRuntime;
using System.Linq;
using System.Text.RegularExpressions;
using System.Web;

public static class Utils
{

    public const string SparseFeatVecColumnsKey = "sfv_cols";
    public const string HeaderSuffixKey = "header_suffix";
    public const string FeaturePrefixKey = "feat_suffix";
    public static RowList listRow = new RowList();

    public static Dictionary<string, string> GetArgs(string[] args)
    {
        var argDict = new Dictionary<string, string>();
        foreach (var arg in args)
        {
            string[] kv = arg.Split(':');
            if (kv.Length == 2)
            {
                argDict[kv[0]] = kv[1];
            }
        }

        if (!argDict.ContainsKey(HeaderSuffixKey))
        {
            argDict[HeaderSuffixKey] = "Headers";
        }

        if (!argDict.ContainsKey(FeaturePrefixKey))
        {
            argDict[FeaturePrefixKey] = "Feat_";
        }

        return argDict;
    }

    static HashSet<string> metaColumns = new HashSet<string>(new string[] { "User_Anid", "Contexts", "Rating" });

    public static HashSet<string> GetMetaColumns()
    {
        return metaColumns;
    }

    public static string GetSparseFeatureName(string prefix, string name, string k)
    {
        return prefix + name.Replace("Features", "_") + k;
    }
}

public class FeatValueTsvGenerator : Processor
{
    public override Schema Produces(string[] requestedColumns, string[] args, Schema inputSchema)
    {
        Schema outputSchema = new Schema();
        HashSet<string> metaColumns = Utils.GetMetaColumns();
        var argDict = Utils.GetArgs(args);
        var prefix = argDict[Utils.FeaturePrefixKey];
        var cols = new HashSet<string>(argDict[Utils.SparseFeatVecColumnsKey].Split(','));
        var headerSuffix = argDict[Utils.HeaderSuffixKey];
        int OFELength = Convert.ToInt32(args[3]);
        int ImplicitLength = Convert.ToInt32(args[4]);

        try
        {
            if (!string.IsNullOrEmpty(prefix))
            {
                foreach (var colInfo in inputSchema.Columns)
                {
                    string name = colInfo.Name;
                    //if (name.StartsWith(prefix))
                    //{
                    //    outputSchema.Add(colInfo);
                    //}
                    //else 
                    if (metaColumns.Contains(name))
                    {
                        // meta data
                        outputSchema.Add(colInfo);
                    }
                    else if (cols.Contains(name))
                    {
                        if (name == "OFEFeatures")
                        {
                            for (int i = 0; i < OFELength; i++)
                            {
                                outputSchema.Add(new ColumnInfo("Feat_" + name + i.ToString(), ColumnDataType.Float));
                            }
                        }
                        else
                        {
                            for (int i = 0; i < ImplicitLength; i++)
                            {
                                outputSchema.Add(new ColumnInfo("Feat_" + name + i.ToString(), ColumnDataType.Float));
                            }
                        }
                    }
                }
            }

        }
        catch (Exception e)
        {
        }
        return outputSchema;
    }

    public override IEnumerable<Row> Process(RowSet input, Row outputRow, string[] args)
    {
        var argDict = Utils.GetArgs(args);
        var prefix = argDict[Utils.FeaturePrefixKey];
        HashSet<string> metaColumns = Utils.GetMetaColumns();
        var sfvCols = new HashSet<string>(argDict[Utils.SparseFeatVecColumnsKey].Split(','));
        var headerSuffix = argDict[Utils.HeaderSuffixKey];
        int OFELength = Convert.ToInt32(args[3]);
        int ImplicitLength = Convert.ToInt32(args[4]);

        foreach (Row row in input.Rows)
        {
            if (!string.IsNullOrEmpty(prefix))
            {
                try
                {
                    foreach (var colInfo in row.Schema.Columns)
                    {
                        string name = colInfo.Name;
                        //if (name.StartsWith(prefix))
                        //{
                        //    // dense features
                        //    outputRow[i].Set(Convert.ToDouble(row[name].String));
                        //    i++;
                        //}
                        //else 
                        if (metaColumns.Contains(name))
                        {
                            // meta data
                            outputRow[name].Set(row[name].Value);
                        }
                        else if (sfvCols.Contains(name))
                        {
                            var ks = row[name + headerSuffix].String;
                            if (row[name].String != null)
                            {
                                // sparse features
                                var kvs = new Dictionary<string, string>();
                                foreach (var kv in row[name].String.Split(' '))
                                {
                                    var ss = kv.Split(':');
                                    if (ss.Length == 2)
                                    {
                                        kvs[ss[0]] = ss[1];
                                    }
                                }
                                if (ks != "N/A")
                                {
                                    var vals = new List<string>();
                                    int i = 0;
                                    foreach (var k in ks.Split(','))
                                    {
                                        if (!string.IsNullOrEmpty(k))
                                        {
                                            var feaName = "Feat_" + name + i.ToString();
                                            if (kvs.ContainsKey(k))
                                            {
                                                outputRow[feaName].Set(float.Parse(kvs[k]));
                                                i++;
                                            }
                                            else
                                            {
                                                outputRow[feaName].Set(0);
                                                i++;
                                            }
                                        }
                                    }
                                }
                            }
                            else if (ks != "N/A")
                            {
                                int i = 0;
                                var feaName = "Feat_" + name + i.ToString();
                                foreach (var k in ks.Split(','))
                                {
                                    if (!string.IsNullOrEmpty(k))
                                    {
                                        outputRow[feaName].Set(0);
                                        i++;
                                    }
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    continue;
                }
                yield return outputRow;
            }
        }
    }
}

public class FeatHeaderTsvOutputter : Outputter
{
    public override void Output(RowSet input, StreamWriter writer, string[] args)
    {
        var argDict = Utils.GetArgs(args);
        var prefix = argDict[Utils.FeaturePrefixKey];
        var sfvCols = new HashSet<string>(argDict[Utils.SparseFeatVecColumnsKey].Split(','));
        var headerSuffix = argDict[Utils.HeaderSuffixKey];

        StringBuilder sb = new StringBuilder();
        HashSet<string> metaColumns = Utils.GetMetaColumns();
        var firstRow = input.Rows.ElementAt(0);

        if (!string.IsNullOrEmpty(prefix))
        {
            foreach (var colInfo in input.Schema.Columns)
            {
                string name = colInfo.Name;
                if (name.StartsWith(prefix))
                {
                    // dense features
                    sb.Append(",").Append(name);
                }
                else if (metaColumns.Contains(name))
                {
                    // meta data
                    sb.Append(",m:").Append(name);
                }
                else if (sfvCols.Contains(name))
                {
                    // sparese features
                    var ks = firstRow[name + headerSuffix].String;
                    if (ks != "N/A")
                    {
                        foreach (var k in ks.Split(','))
                        {
                            if (!string.IsNullOrEmpty(k))
                            {
                                sb.Append(",").Append(Utils.GetSparseFeatureName(prefix, name, k));
                            }
                        }
                    }

                }
            }
        }
        writer.Write(sb.ToString().Trim(','));
        writer.WriteLine();
        writer.Flush();
    }
}