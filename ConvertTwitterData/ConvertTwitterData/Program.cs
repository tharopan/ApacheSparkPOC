using ICSharpCode.SharpZipLib.BZip2;
using System;
using System.Diagnostics;
using System.IO;

namespace ConvertTwitterData
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Convert Twitter data to !");

            //MoveFiles();
            UnZip();
        }

        /// <summary>
        /// Unzip Files
        /// </summary>
        public static void UnZip()
        {
            var rootFolderPath = @"C:\Users\Tharo\Documents\ResearchProject\Data\Twitter-Raw";
            string[] dirs = Directory.GetDirectories(rootFolderPath);

            var files = Directory.GetFiles(rootFolderPath);
            string zPath = @"C:\Program Files\7-Zip\7zG.exe";
            foreach (var file in files)
            {
                Console.WriteLine("Processing File : " + Path.GetFileName(file));
                ProcessStartInfo pro = new ProcessStartInfo();
                pro.WindowStyle = ProcessWindowStyle.Hidden;
                pro.FileName = zPath;
                pro.Arguments = "x \"" + file + "\" -o" + rootFolderPath;
                Process x = Process.Start(pro);
                x.WaitForExit();
            }
        }

        /// <summary>
        /// Move Files to parent directory
        /// </summary>
        public static void MoveFiles()
        {
            var rootFolderPath = @"..\ResearchProject\Data\twitter_stream_2020_04_01\04\01";
            var copyTo = @"..\ResearchProject\Data\twitter_stream_2020_04_01\";

            string[] dirs = Directory.GetDirectories(rootFolderPath);
            foreach (var dir in dirs)
            {
                var files = Directory.GetFiles(dir);

                var dirName = Path.GetFileName(dir);

                foreach (var file in files)
                {
                    var strPath = $"{copyTo}{dirName}_{Path.GetFileName(file)}";
                    System.IO.File.Move(file, strPath);
                }
            }
        }

        public static void ConvertData()
        {
            //using (StreamReader r = new StreamReader("C:\\Users\\Tharo\\Documents\\ResearchProject\\Data\\TwitterInput\\17.json"))
            //{
            //    string json = r.ReadToEnd();
            //    List<Twitter> items = JsonConvert.DeserializeObject<List<Twitter>>(json);
            //}
        }
    }
}
