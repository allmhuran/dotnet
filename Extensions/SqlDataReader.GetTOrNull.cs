using System.Data;
using Microsoft.Data.SqlClient;

namespace Allmhuran.Extensions
{
	public static partial class SqlDataReaderExtensions
	{
		/// <summary>
		/// Attempts to read a SqlDataReader string field as an enum.<br/>
		/// Will ignore any non ascii letter characters in the input string if conversion initially fails.
		/// </summary>
		/// <typeparam name="T">specific type of the enum</typeparam>
		/// <param name="reader"></param>
		/// <param name="fieldOrdinal"></param>
		/// <returns></returns>
		public static T? GetEnumOrNull<T>(this SqlDataReader reader, int fieldOrdinal) where T : struct, Enum
		{
			if (reader.IsDBNull(fieldOrdinal)) return null;

			var str = reader.GetString(fieldOrdinal);

			if (Enum.TryParse<T>(str, true, out var result)) return result;

			var cleaned = new string(str.Where(char.IsAsciiLetter).ToArray());

			return Enum.TryParse<T>(cleaned, true, out result) ? result : null;
		}

		public static string? GetStringOrNull(this SqlDataReader r, int fieldOrdinal)
		{
			return r.IsDBNull(fieldOrdinal) ? null : r.GetString(fieldOrdinal);
		}

		public static char? GetCharOrNull(this SqlDataReader r, int fieldOrdinal)
		{
			return r.IsDBNull(fieldOrdinal) ? null : r.GetChar(fieldOrdinal);
		}

		public static short? GetShortOrNull(this SqlDataReader r, int fieldOrdinal)
		{
			return r.IsDBNull(fieldOrdinal) ? null : r.GetInt16(fieldOrdinal);
		}

		public static int? GetIntOrNull(this SqlDataReader r, int fieldOrdinal)
		{
			return r.IsDBNull(fieldOrdinal) ? null : r.GetInt32(fieldOrdinal);
		}

		public static long? GetLongOrNull(this SqlDataReader r, int fieldOrdinal)
		{
			return r.IsDBNull(fieldOrdinal) ? null : r.GetInt64(fieldOrdinal);
		}

		public static float? GetFloatOrNull(this SqlDataReader r, int fieldOrdinal)
		{
			return r.IsDBNull(fieldOrdinal) ? null : r.GetFloat(fieldOrdinal);
		}

		public static Guid? GetGuidOrNull(this SqlDataReader r, int fieldOrdinal)
		{
			return r.IsDBNull(fieldOrdinal) ? null : r.GetGuid(fieldOrdinal);
		}

		public static double? GetDoubleOrNull(this SqlDataReader r, int fieldOrdinal)
		{
			return r.IsDBNull(fieldOrdinal) ? null : r.GetDouble(fieldOrdinal);
		}

		public static decimal? GetDecimalOrNull(this SqlDataReader r, int fieldOrdinal)
		{
			return r.IsDBNull(fieldOrdinal) ? null : r.GetDecimal(fieldOrdinal);
		}

		public static DateTime? GetDateTimeOrNull(this SqlDataReader r, int fieldOrdinal)
		{
			return r.IsDBNull(fieldOrdinal) ? null : r.GetDateTime(fieldOrdinal);
		}

		public static byte? GetByteOrNull(this SqlDataReader r, int fieldOrdinal)
		{
			return r.IsDBNull(fieldOrdinal) ? null : r.GetByte(fieldOrdinal);
		}

		public static bool? GetBooleanOrNull(this SqlDataReader r, int fieldOrdinal)
		{
			return r.IsDBNull(fieldOrdinal) ? null : r.GetBoolean(fieldOrdinal);
		}
	}
}
