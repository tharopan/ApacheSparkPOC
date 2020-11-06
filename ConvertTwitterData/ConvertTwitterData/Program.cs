using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;

namespace ConvertTwitterData
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Convert Twitter data to !");

        }

        public static void ConvertData()
        {
            using (StreamReader r = new StreamReader("C:\\Users\\Tharo\\Documents\\ResearchProject\\Data\\TwitterInput\\17.json"))
            {
                string json = r.ReadToEnd();
                List<Twitter> items = JsonConvert.DeserializeObject<List<Twitter>>(json);
            }
        }
    }
}
