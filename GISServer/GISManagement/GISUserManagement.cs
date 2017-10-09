using System;
using System.IO;
using System.Collections.Generic;
using System.Runtime.Serialization.Formatters.Binary;
using GISServer.SQLHandler;
using GISServer.Geodata;

namespace GISServer.GISManagement
{
    /// <summary>
    /// 用来处理用户管理指令
    /// </summary>
    public class GISUserManagement
    {
		#region Static Methods
		/// <summary>
		/// 用户授权
		/// </summary>
		/// <returns>成功与否</returns>
		/// <param name="paras">用户名，密码，组织名</param>
		public static byte[] UserAuthorize(string paras)
        {
            string[] msgs = paras.Split(new char[] { '&' });
            string username = msgs[0];
            string password = msgs[1];
            string organization = msgs[2];
            try
            {
                if (GISSQLHandler.HasOrganizationUser(username, password, organization))
                    return Convert.FromBase64String("000");     //成功
                return Convert.FromBase64String("310");    //用户名或密码不对
            }
            catch (Exception e)
            {
                Console.WriteLine(DateTime.Now.ToString() + ":" + e.Message);
                return Convert.FromBase64String("311");      //组织名不对
            }
        }
		/// <summary>
		/// 用户注册
		/// </summary>
		/// <returns>成功与否</returns>
		/// <param name="paras">用户名，密码，组织名</param>
		public static byte[] UserRegister(string paras)
        {
            string[] msgs = paras.Split(new char[] { '&' });
            string username = msgs[0];
            string password = msgs[1];
            string organization = msgs[2];
            if (!GISSQLHandler.hasOrganization(organization))
                GISSQLHandler.CreateNewOrganization(organization);
            try
            {
                GISSQLHandler.addNewUserinfo(username, password, organization);
                return Convert.FromBase64String("000");      //成功
            }
            catch (Exception e)
            {
                Console.WriteLine(DateTime.Now.ToString() + ":" + e.Message);
                return Convert.FromBase64String("310");     //用户名已经存在
            }
        }
        /// <summary>
        /// 获取组织内的所有空间数据元数据
        /// </summary>
        /// <returns>包含所有空间数据元数据的字符串</returns>
        /// <param name="paras">组织和用户名</param>
        public static byte[] GetGeoTables(string paras)
        {
            string[] msgs = paras.Split(new char[] { '&' });
            string username = msgs[0];
            string organization = msgs[1];
            GISSQLHandler gis = new GISSQLHandler();
            string metadata = GISSQLHandler.getGeoTables(organization, username);
            return Convert.FromBase64String(metadata);
        }
        /// <summary>
        /// 上传数据
        /// </summary>
        /// <returns>The geodata.</returns>
        /// <param name="paras">文件名</param>
        /// <param name="data">序列化的空间数据文件</param>
        public static byte[] UploadGeodata(string paras, string data)
        {
            string filename = paras;
            byte[] geodata = Convert.FromBase64String(data);
            webGeodataFile file;
            MemoryStream stream = new MemoryStream(geodata);
            BinaryFormatter BinFormat = new BinaryFormatter();
            try
            {
                file = (webGeodataFile)BinFormat.Deserialize(stream);
            }
            catch(Exception e)
            {
                Console.WriteLine(DateTime.Now.ToString() + ":" + e.Message);
                return Convert.FromBase64String("312");
            }
            finally
            {
                stream.Dispose();
            }
            try
            {
                GISSQLHandler.createGeodataTable(file);
            }
			catch (Exception e)
			{
				Console.WriteLine(DateTime.Now.ToString() + ":" + e.Message);
				return Convert.FromBase64String("310");
			}
            GISSQLHandler.uploadGeodataFile(file);
            return Convert.FromBase64String("Success");
        }
        /// <summary>
        /// 下载数据
        /// </summary>
        /// <returns>The geodata.</returns>
        /// <param name="paras">Paras.</param>
        public static byte[] downloadGeodata(string paras)
        {
            webGeodataFile file;
			string[] msgs = paras.Split(new char[] { '&' });
			string organization = msgs[0];
            string filename = msgs[1];
            try
            {
                file = GISSQLHandler.downloadGeodataFile(organization, filename);
				MemoryStream ms = new MemoryStream();
				BinaryFormatter formatter = new BinaryFormatter();
                formatter.Serialize(ms, file);
				ms.Position = 0;
				byte[] bytes = new byte[ms.Length];
				ms.Read(bytes, 0, bytes.Length);
				ms.Close();
                return System.Text.Encoding.Default.GetBytes("000#" + Convert.ToBase64String(bytes));
            }
            catch(Exception e)
            {
                return System.Text.Encoding.Default.GetBytes("999#");
            }
        }
        #endregion
    }
}
