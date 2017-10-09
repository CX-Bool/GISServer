using System;
using System.IO;
using GISServer.WebServer;

namespace GISServer
{
    /// <summary>
    /// 程序类
    /// </summary>
    class Program
    {
        #region MEMBERS
        /// <summary>
        /// 服务器日志文件路径
        /// </summary>
        static private string logFile = Environment.CurrentDirectory + "/gisserver.log";
        #endregion

        #region EVENT_METHODS
        /// <summary>
        /// 服务器操作响应事件，记录服务器打开，关闭，接收请求的操作日志
        /// </summary>
        /// <param name="msg"></param>
        private static void GISServerRecord(string msg)
        {
            StreamWriter sw = new StreamWriter(logFile);
            sw.WriteLine(DateTime.Now.ToString() + ":" + msg);
            sw.Dispose();
            Console.WriteLine(msg);
        }
        #endregion

        #region MAINMETHODS
        /// <summary>
        /// 服务器程序启动函数，启动监听
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            GISListener listener = new GISListener();
            listener.sendMsgEvent += GISServerRecord;
            listener.Start();
        }
        #endregion
    }
}
