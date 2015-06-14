using ScopeRuntime;
using System.Collections.Generic;
using System.Linq;
public static class Myhelper
{
    public static int MaxDisThreshold = 1000;
    public static int TimeNumber = 48;
    public static int LocationNumber = 3;
    public static string BaseDatestring = "2/22/2015 00:00:00 AM";

    public static bool DateComp(DateTime Date)
    {
        //  DateTime DateCom = Convert.ToDateTime(Date);
        DateTime DateBase = Convert.ToDateTime(BaseDatestring);
        bool result = true;

        if (DateTime.Compare(Date, DateBase) >= 0) result = false;
        else if (DateTime.Compare(Date, DateBase) < 0) result = true;
        return result;
    }

    public static bool DecimalCount(string Lat, string Long)
    {
        bool result = true;
        if (Lat == null || Lat == " " || Lat == "") result = false;
        else
        {
            string[] LatItems = Lat.Split(new char[] { '.' });
            string[] LongItems = Long.Split(new char[] { '.' });
            if (LatItems.Length < 2 || LongItems.Length < 2) result = false;
            else
            {
                if (LatItems[1].Length >= 4 && LongItems[1].Length >= 4) result = true;
                else result = false;
            }

        }
        return result;
    }
    public static bool IsInclude(string ID, string subID)
    {
        if (ID.IndexOf(subID) > -1)
            return true;
        else return false;

    }
    public static int TimeToSlot(DateTime time)
    {
        int Category = 0;
        DayOfWeek dayofweek = time.DayOfWeek;
        if (Convert.ToInt16(dayofweek) == 6 || Convert.ToInt16(dayofweek) == 0)
            Category = 1;
        else Category = 0;
        int hour = time.Hour;

        return (24 * Category + hour);
    }

    public static int LocationType(string Lat, string Long, string MajorLat, string MajorLong, string MinorLat, string MinorLong)//0 for none,1 for majorhubs, 2 for minorhubs
    {
        int result = 0;

        if (Lat == "" || Lat == null || Long == "" || Long == null)
        {
            result = 0;
            return result;
        }
        if ((MajorLat == " " || MajorLat == null) && (MinorLat == " " || MinorLat == null))
        {
            result = 0;
            return result;
        }



        if (MajorLat != " " && MajorLat != null)
        {
            string[] ItemMajorLat = MajorLat.Split(new char[] { ';' });
            string[] ItemMajorLong = MajorLong.Split(new char[] { ';' });
            for (int i = 0; i < ItemMajorLat.Length; i++)
            {

                double dis = Geodist(double.Parse(Lat), double.Parse(Long), double.Parse(ItemMajorLat[i]), double.Parse(ItemMajorLong[i]));
                if (dis < MaxDisThreshold) result = 1;
            }
        }

        if (MinorLat != " " && MinorLat != null)
        {
            string[] ItemMinorLat = MinorLat.Split(new char[] { ';' });
            string[] ItemMinorLong = MinorLong.Split(new char[] { ';' });
            if (result == 0)
            {
                for (int j = 0; j < ItemMinorLat.Length; j++)
                {
                    double dis = Geodist(double.Parse(Lat), double.Parse(Long), double.Parse(ItemMinorLat[j]), double.Parse(ItemMinorLong[j]));
                    if (dis < MaxDisThreshold) result = 2;
                }
            }
        }

        return result;
    }
    public static double Geodist(double lat1, double lon1, double lat2, double lon2)
    {
        // return dist(lat1, lon1, lat2, lon2);
        double radLat1 = Rad(lat1);
        double radLat2 = Rad(lat2);
        double delta_lon = Rad(lon2 - lon1);
        double top_1 = Math.Cos(radLat2) * Math.Sin(delta_lon);
        double top_2 = Math.Cos(radLat1) * Math.Sin(radLat2) - Math.Sin(radLat1) * Math.Cos(radLat2) * Math.Cos(delta_lon);
        double top = Math.Sqrt(top_1 * top_1 + top_2 * top_2);
        double bottom = Math.Sin(radLat1) * Math.Sin(radLat2) + Math.Cos(radLat1) * Math.Cos(radLat2) * Math.Cos(delta_lon);
        double delta_sigma = Math.Atan2(top, bottom);
        double distance = delta_sigma * 6378137.0;
        return distance;
    }

    public static double Rad(double d)
    {
        return d * Math.PI / 180.0;
    }
}







public class Reducer_FilterDuplicate : Reducer
{
    public override Schema Produces(string[] columns, string[] args, Schema input)
    {
        return input.Clone();
    }

    public override IEnumerable<Row> Reduce(RowSet input, Row output, string[] args)
    {
        Dictionary<string, List<Row>> dicRows = new Dictionary<string, List<Row>>();
        foreach (var row in input.Rows)
        {
            var key = row["User_Anid"].String + row["Contexts"].String;
            if (!dicRows.ContainsKey(key))
                dicRows.Add(key, new List<Row>());
            dicRows[key].Add(row);
        }

        foreach (var pair in dicRows)
        {
            var first = pair.Value.First();
            foreach (var col in first.Schema.Columns)
            {
                if (col.Name == "Rating")
                {
                    output[col.Name].Set(
                        pair.Value.Select(dp => Convert.ToDouble(dp[col.Name].Integer)).Average()
                        >= 0.5 ? 1 : 0);
                }
                else
                    output[col.Name].Set(first[col.Name].Value);
            }
            yield return output;
        }
    }
}