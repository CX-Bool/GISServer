using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace GISServer.WebServer
{
    /// <summary>
    /// 监听类
    /// </summary>
    class GISListener
    {
        #region MEMBERS
        /// <summary>
        /// tcp监听对象
        /// </summary>
        private TcpListener tcpListener;
        #endregion

        #region DELEDATE
        /// <summary>
        /// 发送服务器操作消息委托
        /// </summary>
        /// <param name="msg"></param>
        internal delegate void SendListenerMsgDelegate(string msg);
        #endregion

        #region EVENTS
        /// <summary>
        /// 发送服务器操作消息事件
        /// </summary>
        public SendListenerMsgDelegate sendMsgEvent;
        #endregion

        #region Constructors
        /// <summary>
        /// 构造函数，用来设置监听的ip和端口
        /// </summary>
        public GISListener()
        {
            tcpListener = new TcpListener(IPAddress.Any, 1234);
        }
        #endregion

        #region METHODS
        /// <summary>
        /// 开始运行函数
        /// </summary>
        public void Start()
        {
            tcpListener.Start();
            sendMsgEvent?.Invoke("START");
            while (true)
            {
                while (!tcpListener.Pending());
                GISThreadHandler myWorker = new GISThreadHandler(tcpListener);
                Thread myWorkerthread = new Thread(new ThreadStart(myWorker.HandleGISThread));
                myWorkerthread.Name = "Created at" + DateTime.Now.ToString();
                myWorkerthread.Start();
            }
        }
        /// <summary>
        /// 停止运行函数
        /// </summary>
        public void Stop()
        {
            tcpListener.Stop();
            sendMsgEvent?.Invoke("STOP");
        }
        #endregion
    }
}
