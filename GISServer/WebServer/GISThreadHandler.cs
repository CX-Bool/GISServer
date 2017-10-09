using System;
using System.Text;
using System.Net.Sockets;
using GISServer.GISManagement;

namespace GISServer.WebServer
{
    /// <summary>
    /// 消息处理类
    /// </summary>
    class GISThreadHandler
    {
        #region MEMBERS
        /// <summary>
        /// tcp监听对象
        /// </summary>
        private TcpListener tcpListener;
        /// <summary>
        /// tcp网络传输流
        /// </summary>
        private NetworkStream networkStream;
        #endregion

        #region Constructors
        /// <summary>
        /// 构造函数，设置tcp监听对象
        /// </summary>
        /// <param name="tcpListener"></param>
        public GISThreadHandler(TcpListener tcpListener)
        {
            this.tcpListener = tcpListener;
        }
        #endregion

        #region METHODS
        /// <summary>
        /// 网络请求处理函数
        /// </summary>
        public void HandleGISThread()
        {
            TcpClient client = tcpListener.AcceptTcpClient();
            networkStream = client.GetStream();
            try
            {
                string req = GetGISRequest();
                byte[] responseData = GetGISResponceData(req);
                SendGISResponseData(responseData);
            }
            catch (Exception e)
            {
                Console.WriteLine(DateTime.Now.ToString() + ":" + e.Message);
            }
        }
        /// <summary>
        /// 获取Request
        /// </summary>
        /// <returns></returns>
        private string GetGISRequest()
        {
            byte[] streamData = new byte[1024];
            string request = String.Empty;
            while(true)
            {
                int bytesNum = networkStream.Read(streamData, 0, streamData.Length);
                request += Encoding.ASCII.GetString(streamData, 0, bytesNum);
                if (bytesNum <= 0)
                    break;
            }
            return request;
        }
        /// <summary>
        /// 获取Response
        /// </summary>
        /// <param name="req"></param>
        /// <returns></returns>
        private Byte[] GetGISResponceData(string req)
        {
            string[] msgs = req.Split(new char[] { '?' });
            string cmd = msgs[0];
            string op = msgs[1];
            if (cmd == "LOGIN")
                return AllotLoginAssignment(op);
            else if (cmd == "OP")
                return AllotOpAssignment(op);
            return null;
        }
        /// <summary>
        /// 响应用户管理操作，返回响应信息流
        /// </summary>
        /// <returns>The login assignment.</returns>
        /// <param name="cmd">Cmd.</param>
        private Byte[] AllotLoginAssignment(string cmd)
        {
            string[] msgs = cmd.Split(new char[] { '@', '$' });
            string op = msgs[0];
            string paras = msgs[1];
            string data = msgs[2];
            if (op == "SIGNIN")
                return GISUserManagement.UserAuthorize(paras);
            else if (op == "SIGNUP")
                return GISUserManagement.UserRegister(paras);
            else if (op == "UPLOAD")
                return GISUserManagement.UploadGeodata(paras, data);
            else if (op == "DOWNLOAD")
                return GISUserManagement.downloadGeodata(paras);
            else if (op == "SHOW")
                return GISUserManagement.GetGeoTables(paras);
            return null;
        }
        /// <summary>
        /// 响应空间分析操作，返回响应信息流
        /// </summary>
        /// <returns>The op assignment.</returns>
        /// <param name="cmd">Cmd.</param>
        public Byte[] AllotOpAssignment(string cmd)
        {
			string[] msgs = cmd.Split(new char[] { '@', '$' });
			string op = msgs[0];
			string paras = msgs[1];
			string data = msgs[2];
            if (op == "NONE")
                return GISOpManagement.DrawImage();
            else if (op == "FULLEXTENT")
                return GISOpManagement.DrawImage();
            else if (op == "SELECT")
                return GISOpManagement.SelectFeatures();
			else if (op == "FIELD")
				return GISOpManagement.GetDataFields();
			else if (op == "PROPERTY")
				return GISOpManagement.GetDataRecords();
			else if (op == "QUERY")
				return GISOpManagement.GetDataRecords();
            // else if 字段的添加删除修改，记录的修改，空间数据的增加删除修改 NOTIFY
			return null;
        }
        /// <summary>
        /// 发送Response
        /// </summary>
        /// <param name="data"></param>
        private void SendGISResponseData(byte[] data)
        {
            if (networkStream.CanWrite)
                networkStream.Write(data, 0, data.Length);
        }
        #endregion
    }
}
