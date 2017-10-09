using System;
using System.Collections.Generic;
using System.ComponentModel;

namespace GISServer.Geodata
{
    /// <summary>
    /// 传输用空间数据类型
    /// </summary>
    public enum webGeodataType
    {
        [Description("Point")]
        Point = 1,
        [Description("Polyline")]
        Polyline = 3,
        [Description("Polygon")]
        Polygon = 5
    }
    /// <summary>
    /// 传输用空间数据文件
    /// </summary>
    public class webGeodataFile
    {
        /// <summary>
        /// 空间数据文件拥有者
        /// </summary>
		public string username;
        /// <summary>
        /// 空间数据文件组织
        /// </summary>
        public string organization;
        /// <summary>
        /// 空间数据文件名称
        /// </summary>
        public string filename;
        /// <summary>
        /// 空间数据类型
        /// </summary>
        public webGeodataType geoType;
        /// <summary>
        /// 空间数据参考坐标系
        /// </summary>
        public string srid;
        /// <summary>
        /// 空间数据文件字段信息(字段名称+类型)
        /// </summary>
        public List<KeyValuePair<string, Type>> fields = new List<KeyValuePair<string, Type>>();
        /// <summary>
        /// 空间要素数据集合
        /// </summary>
        public List<webGeofeature> geodata = new List<webGeofeature>();

    }
    /// <summary>
    /// 传输用空间要素数据
    /// </summary>
	public class webGeofeature
	{
        /// <summary>
        /// 空间要素数据的编号fid  NOTIFY
        /// </summary>
        public int fid;
        /// <summary>
        /// WKT格式的空间要素数据
        /// </summary>
        public string wktGeodata;
        /// <summary>
        /// 空间要素的属性记录集合
        /// </summary>
        public List<object> records = new List<object>();
	}
}
