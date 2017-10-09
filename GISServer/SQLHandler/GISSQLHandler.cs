using System;
using Npgsql;
using System.Data;
using System.Collections.Generic;
using GISServer.Geodata;

namespace GISServer.SQLHandler
{
    class GISSQLHandler
    {
        #region MEMBERS
        /// <summary>
        /// Database Server IP
        /// </summary>
        private static string dbserver = "localhost";
        /// <summary>
        /// Database Server Port
        /// </summary>
        private static string dbport = "5432";
        /// <summary>
        /// Database UserId
        /// </summary>
        private static string dbuser = "postgres";
        /// <summary>
        /// Database User password
        /// </summary>
        private static string dbpassword = "Cwp19941129";
        /// <summary>
        /// Alphagis Database
        /// </summary>
        private static string dbname = "postgres";
        #endregion

        #region Method
        /// <summary>
        /// The Given Username_Password_Organization Could be Authorized 
        /// </summary>
        /// <returns><c>true</c>If Organization Has this Username_Password<c>false</c>Otherwise</returns>
        /// <param name="username">Username</param>
        /// <param name="password">Password</param>
        /// <param name="organization">Organization</param>
        internal static bool HasOrganizationUser(string username, string password, string organization)
        {
            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();

            string cmdString = "SELECT COUNT(*) FROM " + organization + ".USERINFO WHERE USERNAME='" + username + "' AND PASSWORD='" + password + "'";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdString, con);

            try
            {
                NpgsqlDataReader sdr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                sdr.Read();
                long num = (long)sdr[0];
                sdr.Close();
                if (num == 0)
                    return false;
                return true;
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                cmd.Dispose();
                con.Dispose();
            }
        }
        /// <summary>
        /// Create the New Schema for the New Organization and Add the Table Userinfo and Metadata.
        /// </summary>
        /// <param name="organization">Organization.</param>
        internal static void CreateNewOrganization(string organization)
        {
            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();

            string cmdString = "CREATE SCHEMA " + organization + " AUTHORIZATION POSTGRES;";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdString, con);
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE TABLE " + organization + ".userinfo(username character varying(40),password character varying(20),organization character varying(40),PRIMARY KEY (username));";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "CREATE TABLE " + organization + ".metadata(name character varying(40),owner character varying(40),type character varying(10),srid character varying(10),PRIMARY KEY (name));";
            cmd.ExecuteNonQuery();
            cmd.Dispose();
            con.Dispose();
        }
        /// <summary>
        /// Does the Database Have the Schema for the Organization
        /// </summary>
        /// <returns><c>true</c>Organization has Registered<c>false</c>Otherwise</returns>
        /// <param name="organization">Organization.</param>
        internal static bool hasOrganization(string organization)
        {
            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();

            string cmdString = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES where table_schema='" + organization + "'";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdString, con);
            NpgsqlDataReader sdr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
            sdr.Read();
            long num = (long)sdr[0];
            sdr.Close();
            cmd.Dispose();
            con.Dispose();
            if (num == 0)
                return false;
            return true;
        }
        /// <summary>
        /// Add the New Userinfo
        /// </summary>
        /// <param name="username">Username</param>
        /// <param name="password">Password</param>
        /// <param name="organization">Organization</param>
        internal static void addNewUserinfo(string username, string password, string organization)
        {

            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();

            string cmdString = "insert into " + organization + ".userinfo values ('" + username + "','" + password + "','" + organization + "');";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdString, con);
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                cmd.Dispose();
                con.Dispose();
            }
        }
        /// <summary>
        /// Get All GeoData MetaData of Organization
        /// </summary>
        /// <returns>The MetaData(name,owner,tyep)</returns>
        /// <param name="organization">Organization</param>
        /// <param name="username">Username</param>
        internal static string getGeoTables(string organization, string username)
        {
            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();

            string cmdString = "SELECT * FROM " + organization + ".metadata";
            if (organization == "public")
                cmdString += " where owner=" + username;
            NpgsqlCommand cmd = new NpgsqlCommand(cmdString, con);
            NpgsqlDataReader sdr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
            string metadata = string.Empty;
            while (sdr.Read())
            {
                metadata += sdr["name"] + "#" + sdr["owner"] + "#" + sdr["type"] + "#" + sdr["srid"] + "&";
            }
            sdr.Close();
            cmd.Dispose();
            con.Dispose();
            return metadata;
        }
        #endregion

        #region GeoMethod
        /// <summary>
        /// Creates the Geodata Table.
        /// </summary>
        /// <returns><c>true</c>Success<c>false</c>Otherwise</returns>
        /// <param name="organization">Organization</param>
        /// <param name="username">Username</param>
        /// <param name="filename">Filename</param>
        /// <param name="fields">Fields</param>
        /// <param name="geoType">GeoType</param>
        /// <param name="srid">Srid</param>
        public static void createGeodataTable(webGeodataFile file)
        {
            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();
            NpgsqlCommand cmd = con.CreateCommand();

            try
            {
                string cmdString = "CREATE TABLE " + file.organization + "." + file.filename + "_A(";
                for (int i = 0; i < file.fields.Count; i++)
                {
                    if (file.fields[i].Value == typeof(int))
                        cmdString += file.fields[i].Key + " INTEGER,";
                    else if (file.fields[i].Value == typeof(double))
                        cmdString += file.fields[i].Key + "DOUBLE PRECISION,";
                    else if (file.fields[i].Value == typeof(string))
                        cmdString += file.fields[i].Key + "TEXT,";
                }
                cmd.CommandText = cmdString.Substring(0, cmdString.Length - 1) + ");";
                cmd.ExecuteNonQuery();

                cmdString = "CREATE TABLE " + file.organization + "." + file.filename + "(FID INTEGER";
                if (file.geoType == webGeodataType.Point)
                    cmdString += ",geom geometry(POINT," + file.srid + ");";
                else if (file.geoType == webGeodataType.Polyline)
                    cmdString += ",geom geometry(LINESTRING," + file.srid + ");";
                else if (file.geoType == webGeodataType.Polygon)
                    cmdString += ",geom geometry(POLYGON," + file.srid + ");";
                cmd.CommandText = cmdString;
                cmd.ExecuteNonQuery();

                cmd.CommandText = "INSERT INTO " + file.organization + ".metadata values('" + file.filename + "','" + file.username + "','" + file.geoType.ToString() + "','" + file.srid + "');";
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine(DateTime.Now.ToString() + ":" + e.Message);
                throw e;
            }
            finally
            {
                cmd.Dispose();
                con.Dispose();
            }
        }
        /// <summary>
        /// Upload the Geodata into the geotable
        /// </summary>
        /// <param name="file">File</param>
        public static void uploadGeodataFile(webGeodataFile file)
        {
            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();
            NpgsqlCommand cmd = con.CreateCommand();

            try
            {
                string cmdString = string.Empty;
                for (int i = 0; i < file.geodata.Count; i++)
                    cmdString += "INSERT INTO " + file.organization + "." + file.filename + "_A VALUES(" + GenerateRecordsSQL(file.fields, file.geodata[i].records) + ");";
                cmd.CommandText = cmdString;
                cmd.ExecuteNonQuery();

                cmdString = string.Empty;
                for (int i = 0; i < file.geodata.Count; i++)
                    cmdString += "INSERT INTO " + file.organization + "." + file.filename + " VALUES(" + file.geodata[i].fid.ToString() + ",ST_GeometryFromText(" + file.geodata[i].wktGeodata + "));";
                cmd.CommandText = cmdString;
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine(DateTime.Now.ToString() + ":" + e.Message);
                throw e;
            }
            finally
            {
                cmd.Dispose();
                con.Dispose();
            }
        }
        /// <summary>
        /// Downloads the Geodata.
        /// </summary>
        /// <returns>A Instance of webGeodataFile from the Geodata with the Organization and the Filename</returns>
        /// <param name="organization">Organization</param>
        /// <param name="filename">Filename</param>
        public static webGeodataFile downloadGeodataFile(string organization, string filename)
        {
            webGeodataFile file = new webGeodataFile();
            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();
            try
            {
                SetGeodataFileHeader(file, organization, filename, con);
                SetGeodataFileBody(file, organization, filename, con);
                return file;
            }
            catch (Exception e)
            {
                Console.WriteLine(DateTime.Now.ToString() + ":" + e.Message);
                throw e;
            }
            finally
            {
                con.Dispose();
            }
        }
        /// <summary>
        /// Set the Geodata Header.
        /// </summary>
        /// <param name="file">File</param>
        /// <param name="organization">Organization</param>
        /// <param name="filename">Filename</param>
        /// <param name="con">Postgre Connection</param>
        public static void SetGeodataFileHeader(webGeodataFile file, string organization, string filename, NpgsqlConnection con)
        {
            try
            {
                string cmdString = "SELECT * FROM " + organization + ".METADATA WHERE NAME='" + filename + "';";
                NpgsqlCommand cmd = new NpgsqlCommand(cmdString, con);
                NpgsqlDataReader sdr = cmd.ExecuteReader();
                sdr.Read();
                file.filename = sdr["name"].ToString();
                file.username = sdr["owner"].ToString();
                file.geoType = (webGeodataType)Enum.Parse(typeof(webGeodataType), sdr["type"].ToString());
                file.srid = sdr["srid"].ToString();
                sdr.Close();

                cmd.CommandText = "SELECT column_name ,data_type FROM information_schema.columns WHERE table_schema = '" + organization + "' and table_name='" + filename + "_a'";
                sdr = cmd.ExecuteReader();
                while (sdr.Read())
                {
                    if (sdr["data_type"].ToString() == "integer")
                        file.fields.Add(new KeyValuePair<string, Type>(sdr["column_name"].ToString(), typeof(int)));
                    else if (sdr["data_type"].ToString() == "double precision")
                        file.fields.Add(new KeyValuePair<string, Type>(sdr["column_name"].ToString(), typeof(double)));
                    else if (sdr["data_type"].ToString() == "text")
                        file.fields.Add(new KeyValuePair<string, Type>(sdr["column_name"].ToString(), typeof(string)));
                }
                sdr.Close();
                cmd.Dispose();
            }
            catch (Exception e)
            {
                throw e;
            }
        }
        /// <summary>
        /// Set the Geodata Body.
        /// </summary>
        /// <param name="file">File</param>
        /// <param name="organization">Organization</param>
        /// <param name="filename">Filename</param>
        /// <param name="con">Postgre Connection</param>
        public static void SetGeodataFileBody(webGeodataFile file, string organization, string filename, NpgsqlConnection con)
        {
            try
            {
                string cmdRecords = "SELECT * FROM " + organization + "." + filename + "_A;";
                NpgsqlDataAdapter adpRecords = new NpgsqlDataAdapter(cmdRecords, con);
                DataSet dsRecords = new DataSet();
                adpRecords.Fill(dsRecords);
                DataTable dtRecords = dsRecords.Tables[0];
                adpRecords.Dispose();
                dsRecords.Dispose();

                string cmdGeodata = "SELECT FID,ST_ASTEXT(geom) FROM " + organization + "." + filename + ";";
                NpgsqlDataAdapter adpGeodata = new NpgsqlDataAdapter(cmdGeodata, con);
                DataSet dsGeodata = new DataSet();
                adpGeodata.Fill(dsGeodata);
                DataTable dtGeodata = dsGeodata.Tables[0];
                adpGeodata.Dispose();
                dsGeodata.Dispose();

                for (int i = 0; i < dtRecords.Rows.Count; i++)
                {
                    webGeofeature f = new webGeofeature();
                    f.fid = (int)dtGeodata.Rows[i][0];
                    f.wktGeodata = dtGeodata.Rows[i][1].ToString();
                    for (int j = 0; j < dtRecords.Columns.Count; j++)
                        f.records.Add(dtRecords.Rows[i][j]);
                    file.geodata.Add(f);
                }

                dtRecords.Dispose();
                dtGeodata.Dispose();
            }
            catch (Exception e)
            {
                throw e;
            }
        }
		/// <summary>
		/// Generate the SQL for the Records of One Feature, for the Update or Insert the Records of One Feature
		/// </summary>
		/// <returns>SQL</returns>
		/// <param name="fields">Fields</param>
		/// <param name="records">Records</param>
		internal static string GenerateRecordsSQL(List<KeyValuePair<string, Type>> fields, List<object> records)
		{
			string updateSQL = string.Empty;
			for (int i = 0; i < fields.Count; i++)
			{
				if (fields[i].Value == typeof(string))
					updateSQL += "'" + records[i] + "',";
				else if (fields[i].Value == typeof(double) || fields[i].Value == typeof(int))
					updateSQL += records[i] + ",";
			}
			return updateSQL.Substring(0, updateSQL.Length - 1);
		}
        #endregion

        #region TODO 字段记录空间数据的增删改查
        /// <summary>
        /// 添加字段
        /// </summary>
        public void AddField(string organization, string filename, string fieldname, Type fieldType)
        {
            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();
            string cmdString = string.Empty;
            if (fieldType == typeof(int))
                cmdString = "ALTER TABLE " + organization + "." + filename + " ADD COLUMN " + fieldname + " integer;";
            else if (fieldType == typeof(double))
                cmdString = "ALTER TABLE " + organization + "." + filename + " ADD COLUMN " + fieldname + " double precision;";
            else
                cmdString = "ALTER TABLE " + organization + "." + filename + " ADD COLUMN " + fieldname + " text;";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdString, con);
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                cmd.Dispose();
                con.Dispose();
            }
        }
        /// <summary>
        /// 删除字段
        /// </summary>
        internal void DelField(string organization, string filename, string fieldname)
        {
            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();
            string cmdString = "ALTER TABLE " + organization + "." + filename + " DROP COLUMN " + fieldname + ";";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdString, con);
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                cmd.Dispose();
                con.Dispose();
            }
        }
        /// <summary>
        /// 修改字段
        /// </summary>
        internal void RectifyField(string organization, string filename, string oldname, string newname)
        {
            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();
            string cmdString = "ALTER TABLE " + organization + "." + filename + " RENAME " + oldname + " TO " + newname + " ;";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdString, con);
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                cmd.Dispose();
                con.Dispose();
            }
        }
        /// <summary>
        /// 获取字段
        /// </summary>
        /// <returns></returns>
        internal string GetFields(string organization, string filename)
        {
            string fields = string.Empty;

            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();

            string cmdString = "SELECT column_name ,data_type FROM information_schema.columns WHERE table_schema = '" + organization + "' and table_name='" + filename + "_a'";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdString, con);
            NpgsqlDataReader sdr = cmd.ExecuteReader();
            while (sdr.Read())
            {
                fields += sdr["column_name"] + "#" + sdr["data_type"] + "&";
            }
            sdr.Close();
            cmd.Dispose();
            return null;
        }
        /// <summary>
        /// 修改记录
        /// </summary>
        internal void RecityRecord(string organization, string filename, List<KeyValuePair<string, Type>> fields, List<List<object>> records)
        {
            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();

            NpgsqlCommand cmd = con.CreateCommand();
            try
            {
                string cmdString = "delete from " + organization + "." + filename + ";";
                cmd.CommandText = cmdString;
                cmd.ExecuteNonQuery();

                cmdString = string.Empty;
                for (int i = 0; i < records.Count; i++)
                    cmdString += "INSERT INTO " + organization + "." + filename + "_A VALUES(" + GenerateRecordsSQL(fields, records[i]) + ");";
                cmd.CommandText = cmdString;
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine(DateTime.Now.ToString() + ":" + e.Message);
                throw e;
            }
            finally
            {
                cmd.Dispose();
                con.Dispose();
            }
        }
        /// <summary>
        /// 获取符合条件的所有记录
        /// </summary>
        /// <returns>The records.</returns>
        /// <param name="organization">Organization.</param>
        /// <param name="filename">Filename.</param>
        internal DataTable GetRecords(string organization, string filename, string sql)
        {
            string fields = string.Empty;

            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();

            string cmdString = "SELECT * FROM " + organization + "." + filename + "_a " + sql + ";";
            NpgsqlDataAdapter adpRecords = new NpgsqlDataAdapter(cmdString, con);
            DataSet dsRecords = new DataSet();
            adpRecords.Fill(dsRecords);
            DataTable dtRecords = dsRecords.Tables[0];
            adpRecords.Dispose();
            dsRecords.Dispose();
            return dtRecords;
        }
        /// <summary>
        /// 添加几何形状
        /// </summary>
        internal static void AddGeometry(string organization, string filename, string fid, string wktdata)
        {
            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();
            string cmdString = "insert into " + organization + "." + filename + " values(" + fid + ",ST_GeometryFromText(" + wktdata + "));";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdString, con);
            cmd.ExecuteNonQuery();

            cmd.Dispose();
            con.Dispose();
        }
        /// <summary>
        /// 删除多边形
        /// </summary>
        internal void DelGeometry(string organization, string filename, string fid, string wktdata)
        {
            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();
            string cmdString = "DELETE From " + organization + "." + filename + " WHERE fid = " + fid + ";";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdString, con);
            cmd.ExecuteNonQuery();

            cmd.Dispose();
            con.Dispose();
        }
        /// <summary>
        /// 修改多边形
        /// </summary>
        internal void RectifyGeometry(string organization, string filename, string fid, string wktdata)
        {
            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();
            string cmdString = "UPDATE " + organization + "." + filename + " SET geom = ST_GeometryFromText(" + wktdata + ") WHERE fid = " + fid + ";";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdString, con);
            cmd.ExecuteNonQuery();

            cmd.Dispose();
            con.Dispose();
        }
        #endregion

        #region TODO 空间数据的空间分析
        /// <summary>
        /// GET the Selected Features
        /// </summary>
        /// <returns>The Selected Features</returns>
        /// <param name="organization">Organization</param>
        /// <param name="filename">Filename</param>
        /// <param name="wktMBR">The Selection Area</param>
        internal List<int> SelectGeometry(string organization, string filename, string wktMBR)
        {
            string conString = "Server=" + dbserver + ";Port=" + dbport + ";UserId=" + dbuser + ";Password=" + dbpassword + ";Database=" + dbname + ";";
            NpgsqlConnection con = new NpgsqlConnection(conString);
            con.Open();

            List<int> fids = new List<int>();
            string cmdString = "SELECT fid FROM " + organization + "." + filename + " WHere ST_Intersects(geom, ST_GeometryFromText(" + wktMBR + "));";
            NpgsqlCommand cmd = new NpgsqlCommand(cmdString, con);
            NpgsqlDataReader sdr = cmd.ExecuteReader();
            while (sdr.Read())
            {
                fids.Add((int)sdr[0]);
            }
            sdr.Close();
            cmd.Dispose();
            return fids;
        }
        #endregion
    }
}
