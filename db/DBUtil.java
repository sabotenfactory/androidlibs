package jp.gr.java_conf.saboten.androidlib.db;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;


/**
 * Sqliteデータベースでのデータ管理用のユーティリティクラス.<br/>
 * <p>
 * USAGE
 * <p>
 * <ol>
 * <li>テーブル数分の定義クラス（{@link Table}）を作成する。
 * <li>{@link SQLiteOpenHelper}を用意する。
 * <pre>
 * public static class DBAdapter extends SQLiteOpenHelper {
 *
 *     private static DBAdapter instance = null;
 *
 *     public static synchronized DBAdapter getInstance(Context context) {
 *         if (instance == null)
 *             instance = new DBAdapter(context);
 *         return instance;
 *     }
 *
 *     private DBAdapter(Context context) {
 *         super(context, App.DB_NAME, null, App.DB_VERSION);
 *     }
 *     {@code @Override}
 *     public void onCreate(SQLiteDatabase db) {
 *         db.execSQL(DBUtil.getCreateSql(UserMaster.class));
 *         db.execSQL(DBUtil.getCreateSql(Rank.class));
 *         db.execSQL(DBUtil.getCreateSql(Result.class));
 *     }
 *     {@code @Override}
 *     public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
 *     }
 * }
 * </pre>
 * <li>ユーティリティメソッドを使用したコードを記述する。
 * </ol>
 */
public class DBUtil {

	private static final String LOG_TAG = DBUtil.class.getSimpleName();

	/*
	 * ●sqliteの制約
	 * そもそもカラムにおける型の制約はない
	 * カラムは型ではなく、アフィニティ（親和性）を持つ。それに応じたデータ型に変換されて格納される
	 * TEXT		数値は文字列として格納。文字列はそのまま
	 * NUMERIC	数値はINTEGERまたはREALとして格納。その他は？
	 * INTEGER	少数値→整数に変換。文字列はそのまま格納？
	 * REAL		数値はINTEGERまたはREALとして格納。その他は？
	 * NONE		完全に追加されるデータの型に従う
	 *
	 * 内部的に扱うデータの型は以下の５つ
	 * ・NULL	NULL値
	 * ・INTEGER	負号付きの整数 (サイズに応じて1、2、3、4、6、8バイト)
	 * 			※8バイト＝java.lang.Long
	 * ・REAL	浮動小数点数 (8バイトのIEEE)
	 * ・TEXT	データベースのエンコーディングにエンコードされた文字列(UTF-8、UTF-16BE、UTF-16-LE)
	 * ・BLOB	エンコードされない文字列
	 *
	 * AUTO_INCREMENT
	 *  INTEGER PRIMARY KEYなカラムにnullでInsertすると自動採番される
	 *  ※但し同一テーブルで一つまで
	 *
	 * ●当Utilityクラスの仕様
	 * Booleanは、TEXTを使用：Booleanから"true"or"false"に変換格納する
	 * DateはTEXTを使用：yyyyMMddHHmmssSSSの文字列
	 * BigDecimalはTEXTを使用：BigDecimal.toStringとコンストラクタで相互変換
	 *
	 */


	/**
	 * テーブル定義クラス.<br/>
	 * 一つのクラスが一つのテーブルに、
	 * publicフィールドが各カラムに対応する。<br/>
	 * 尚、末尾が"_"で終わるフィールドは無視される。
	 * <pre>
	 * public static class UserMaster extends Table {
	 *     public Long _id;
	 *     public String name;
	 *     public Date created;
	 * }
	 * public static class Rank extends Table {
	 *     public Long _id;
	 *     public int rank;
	 *     public int notColumnField_;
	 * }
	 * public static class Result extends Table {
	 *     public int userId;
	 *     public int gameId;
	 *     public int point;
	 *     {@code @Override}
	 *     public String[] getPrimaryKeyFieldNames() {
	 *         return new String[]{"USER_ID", "GAME_ID"};
	 *     }
	 * }
	 * </pre>
	 *
	 * <p>
	 * このクラス構造から、対応するテーブルのCREATE文を作成（{@link DBUtil#getCreateSql(Class)}）する事が可能。<br/>
	 * また{@link DBUtil#list(Class, String, Object...)}等の検索メソッドを使って、検索結果をそれぞれのクラスのインスタンスとして取得する事や、<br/>
	 * インスタンスを使って{@link DBUtil#insert(SQLiteDatabase, Table)}や{@link DBUtil#update(SQLiteDatabase, Table)}等のユーティリティメソッドを使用する事が可能。
	 * <p>
	 * <>
	 * publicフィールドとして使用可能な型は以下に限定する。これらはSqliteがサポートする型と変換される前提であるため。
	 * <ul>
	 * <li>Integer,int
	 * <li>Long,long
	 * <li>Double,double
	 * <li>String
	 * <li>boolean:sqliteはサポートしないが、TEXT型で代替。値は"true"or"false"に変換する。
	 * <li>Date:sqliteはサポートしないが、TEXT型で代替。値は"yyyyMMddHHmmssSSS"の文字列に変換する。
	 * <li>BigDecimal:sqliteはサポートしないが、TEXT型で代替。BigDecimal.toStringとコンストラクタで相互変換する。
	 * <li>byte[]
	 * </ul>
	 * プリミティブ型を使用した場合は、自動的に"NOTNULL"制約が付加される。<br/>
	 * 数値をIntegerとするかLongとするかは、必要に応じて決定すればよいが、
	 * 自動採番型の場合はLongにしておく事をお勧めする。<br/>
	 * （※AndroidのAPIを使ってInsertすると、自動採番値が返ってくるが、それがLong型のため）
	 * <p>
	 * Cursor処理と単一主キーカラム<br/>
	 * 処理結果に対してCursorで処理する場合、<br/>
	 * Cursorは、取得結果のカラムに"_id"という名前の、ID値を格納したカラムがある事を前提としているので、<br/>
	 * テーブルの主キーが単一カラムである場合は、常に「_id」という名前にしておくとCursorを使う時に面倒が無い、というメリットがある。<br/>
	 * <p>
	 * ※予約後と被らないように注意する事 http://mamehsp.blog66.fc2.com/blog-entry-11.html
	 *
	 *
	 */
	public abstract static class Table {

		/**
		 * DB上のテーブル名を返却する<br/>
		 * デフォルトは、
		 * <ul>
		 * <li>クラス名：キャメルケース
		 * <li>テーブル名：大文字、アンダーバー区切り
		 * </ul>
		 * を前提として変換した文字列。
		 */
		public String getTableName() {
			return unCamelize(this.getClass().getSimpleName());
		}

		/**
		 * プライマリーキーのカラム名を返却する<br/>
		 * デフォルトは、"_id"という名前のカラム（なければException）。<br/>
		 * それ以外のカラムや、複合カラムで構成する場合は、オーバーライドして下さい。
		 */
		public String[] getPrimaryKeyFieldNames() {
			// check!
			try {
				this.getClass().getField("_id");
			} catch (SecurityException e) {
				throw new RuntimeException(e);
			} catch (NoSuchFieldException e) {
				throw new RuntimeException("Please override this method.");
			}
			return new String[]{"_id"};
		}
	}

	/*
	 * "_id"カラムに関して
	 *
	 * AndroidのCursorが機能するには、ResultSet内に"_id"というカラムが必須
	 * そのため、テーブルに「_id」というカラムを用意（してそのカラムを含むResultSetを取得）するか、
	 * ResultSetにデフォルトで含まれる「ROWID」を「as _id」で別名定義して取り出す。
	 *
	 * Select文を生成する際に、"_id"フィールドが無い場合は、自動的に「ROWID as _ID」を付加する
	 *  ※フィールド名"_id"は、INTEGER PRIMARY KEY AUTOINCREMENT で固定
	 *
	 *
	 */


	/**
	 * Tableクラスの構造を元に、CREATE文を生成する。
	 */
	public static String getCreateSql(Class<? extends Table> clazz) {
		String tblName = unCamelize(clazz.getSimpleName());
//		Log.d(LOG_TAG, tblName + ":" + clazz);

		StringBuilder sb = new StringBuilder();
		sb.append("create table ").append(tblName).append("(");

		for (Field f : clazz.getFields()) {

			if (Modifier.isStatic(f.getModifiers()))
				continue;

			String fieldName = f.getName();
			if (fieldName.endsWith("_"))
				continue;

			Class<?> fieldType = f.getType();
//			Log.d(LOG_TAG, fieldName + ":" + fieldType);
			String exp = "";
			if (fieldName.equals("_id")) {
				if (fieldType != Long.class)
					throw new RuntimeException("field '_id' must be Long.");
				exp = "_ID INTEGER PRIMARY KEY AUTOINCREMENT";
			} else {
				String colName = unCamelize(fieldName);
				if (fieldType == Integer.class
				|| fieldType == Long.class
				|| fieldType == Date.class)
					exp = colName + " INTEGER";
				else
				if (fieldType == int.class
				|| fieldType == long.class)
					exp = colName + " INTEGER NOT NULL";
				else
				if (fieldType == String.class
				|| fieldType == BigDecimal.class)
					exp = colName + " TEXT";
				else
				if (fieldType == boolean.class)
					exp = colName + " TEXT NOT NULL";
				else
				if (fieldType == Double.class)
//					exp = colName + " REAL";
					exp = colName + " TEXT";
				else
				if (fieldType == double.class)
					exp = colName + " REAL NOT NULL";
				else
				if (fieldType == byte[].class)
					exp = colName + " BLOB";
				else
					throw new RuntimeException("Unavailable Field Type : " + fieldName + " = " + fieldType);
			}
			sb.append(exp).append(",");
		}

		Table t = newInstance(clazz);
		String[] keys = t.getPrimaryKeyFieldNames();
		if (keys == null || keys.length == 0
		|| (keys.length == 1 && keys[0].equals("_id"))) {
			sb.deleteCharAt(sb.length() - 1);
		} else {
			// TODO "_id"の場合に二重定義しても問題無いのか？
			sb.append("primary key (");
			for (String k : keys) {
//				String colName = unCamelize(k);
				String colName = k;
				sb.append(colName).append(",");
			}
			sb.deleteCharAt(sb.length() - 1);
			sb.append(")");
		}

		sb.append(")");
//		Log.d(LOG_TAG, sb.toString());
		return sb.toString();
	}

	// clazz.getFields() の順序性が保証されないので使えない
//	public static String getInsertSql(Class<? extends Table> clazz) {
//		String tblName = unCamelize(clazz.getSimpleName());
//		StringBuilder sb = new StringBuilder();
//		sb.append("insert into ").append(tblName).append("(");
//		for (Field f : clazz.getFields()) {
//			String fieldName = f.getName();
//			String colName = unCamelize(fieldName);
//			sb.append(colName).append(",");
//		}
//		sb.deleteCharAt(sb.length() - 1);
//		sb.append(") values (");
//		for (@SuppressWarnings("unused") Field f : clazz.getFields()) {
//			sb.append("?,");
//		}
//		sb.deleteCharAt(sb.length() - 1);
//		sb.append(")");
//		return sb.toString();
//	}

	/**
	 * テーブルインスタンスを文字列化する。主に、DBから取得したレコード内容のロギングに使用する。
	 */
	public static String toString(Table tbl) {
		Class<?> clazz = tbl.getClass();
		StringBuilder sb = new StringBuilder();
		sb.append(clazz.getSimpleName()).append("[");
		for (Field f : clazz.getFields()) {
			if (Modifier.isStatic(f.getModifiers()))
				continue;
			String fieldName = f.getName();
			Object obj = get(tbl, f);
			String val = obj == null ? "<null>" : obj.toString();
			sb.append(fieldName).append("=").append(val).append(",");
		}
		sb.append("]");
		return sb.toString();
	}

	/* ***********************************************************************************/
	/* Persistor */

	public static <T extends Table> long insert(SQLiteDatabase db, T t) {
//		String tableName = unCamelize(t.getClass().getSimpleName());
		ContentValues values = convert(t);
		return insert(db, t.getTableName(), values);
	}

	public static <T extends Table> int update(SQLiteDatabase db, T t) {
//		String tableName = unCamelize(t.getClass().getSimpleName());
		ContentValues values = convert(t);
		return update(db, t.getTableName(), values, getWhereClause(t), getWhereArgs(t));
	}

	public static <T extends Table> int delete(SQLiteDatabase db, T t) {
//		String tableName = unCamelize(t.getClass().getSimpleName());
		return delete(db, t.getTableName(), getWhereClause(t), getWhereArgs(t));
	}

	private static <T extends Table> String getWhereClause(T t) { // TODO キャッシュ
		String[] keys = t.getPrimaryKeyFieldNames();
		if (keys == null || keys.length == 0)
			return "";
		StringBuilder sb = new StringBuilder();
		for (String fieldName : keys) {
			sb.append(unCamelize(fieldName)).append("=? and ");
		}
//		sb.deleteCharAt(sb.length() - 1);
		sb.delete(sb.length() - 4, sb.length());
		return sb.toString();
	}

	private static <T extends Table> String[] getWhereArgs(T t) { // TODO キャッシュ
		String[] keys = t.getPrimaryKeyFieldNames();
		if (keys == null || keys.length == 0)
			return null;
		List<String> ret = new ArrayList<String>();
		for (String fieldName : t.getPrimaryKeyFieldNames()) {
			Field f = getField(t.getClass(), fieldName);
			Object v = get(t, f);
			ret.add(v.toString());
		}
		return ret.toArray(new String[0]);
	}


	/* ***********************************************************************************/
	/* Select */

//	public static <T extends Table> Cursor select(SQLiteDatabase db, Class<T> tbl,
//			String whereClause, String...whereArgs) {
//		return db.rawQuery(getSelectSql(tbl, whereClause), whereArgs);
//	}

	public static class TableParseCallback<T extends Table> implements IListCallback<T> {
		private Class<T> clazz;
		public TableParseCallback(Class<T> clazz) {
			this.clazz = clazz;
		}
		@Override
		public T process(Cursor c) {
			T ret = parseCursor(clazz, c);
//			Log.d(LOG_TAG, DBUtil.toString(ret));
			return ret;
		}
	}

//	public static <T extends Table> List<T> list(final Class<T> clazz,
//			String whereClause, Object...whereArgs) {
//		return list(clazz, whereClause, null, whereArgs);
//	}
//
//	public static <T extends Table> List<T> list(final Class<T> clazz,
//			String whereClause, String orderClause, Object...whereArgs) {
//		return /*DBAdapter.*/list(new TableParseCallback<T>(clazz),
//				getSelectSql(clazz, whereClause, orderClause), whereArgs);
//	}

	public static <T extends Table> List<T> list(SQLiteDatabase db, final Class<T> clazz,
			String whereClause, Object...whereArgs) {
		return list(db, clazz, whereClause, null, whereArgs);
	}

	public static <T extends Table> List<T> list(SQLiteDatabase db, final Class<T> clazz,
			String whereClause, String orderClause, Object...whereArgs) {
		return /*DBAdapter.*/list(db, new TableParseCallback<T>(clazz),
				getSelectSql(clazz, whereClause, orderClause), whereArgs);
	}

	protected static String getSelectSql(Class<? extends Table> clazz, String whereClause, String orderClause) {
		String tableName = unCamelize(clazz.getSimpleName()); // TODO キャッシュ
		StringBuilder sb = new StringBuilder();
		sb.append("select ");
//		if (!hasField(clazz, "_id")) TODO Cursorが機能するには、ResultSet内に"_id"というカラムが必須
//			sb.append("ROWID as _id,");
		sb.append("* from ").append(tableName);
		if (whereClause != null)
			sb.append(" where ").append(whereClause);
		if (orderClause != null)
			sb.append(" ").append(orderClause);
//		sb.append(" limit 1000 offset 0");
		return sb.toString();
	}

	public static String where(Object value) {
		if (value instanceof Date) {
			return formatDate((Date)value);
		} else {
			return value.toString();
		}
	}

	/* ***********************************************************************************/
	/* Transaction */

	public interface ITransaction<T> {
		T exec(SQLiteDatabase db);
	}

	public static <T> T doInTransaction(SQLiteDatabase db, ITransaction<T> callback) {
		db.beginTransaction();
//		Log.d(LOG_TAG, "*** begin transaction");
		try {
			T ret = callback.exec(db);
			db.setTransactionSuccessful();
//			Log.d(LOG_TAG, "*** transaction success");
			return ret;
		} catch (RuntimeException e) {
			Log.e(LOG_TAG, "", e);
//			Log.d(LOG_TAG, "*** transaction exceptioned");
			return null;
		} finally {
			db.endTransaction();
//			Log.d(LOG_TAG, "*** end transaction");
//			DBAdapter.close();
		}
	}




	/* ***********************************************************************************/
	/* Utility method */

	public static <T extends Table> T parseCursor(Class<T> clazz, Cursor c) {
		T t = newInstance(clazz);
		for (Field f : clazz.getFields()) {
			if (Modifier.isStatic(f.getModifiers()))
				continue;
			String fieldName = f.getName();
			if (fieldName.endsWith("_"))
				continue;
			Class<?> fieldType = f.getType();
			String colName = unCamelize(fieldName);
			Object val = null;
			int colIndex = c.getColumnIndex(colName);
			if (colIndex == -1) {
//				Log.d(LOG_TAG, "colName missing : " + colName);
				continue;
			}
			if (fieldType == Integer.class
			|| fieldType == int.class)
				val = c.getInt(colIndex);
			else
			if (fieldType == Long.class
			|| fieldType == long.class)
				val = c.getLong(colIndex);
			else if (fieldType == String.class)
				val = c.getString(colIndex);
			else if (fieldType == boolean.class)
				val = Boolean.valueOf(c.getString(colIndex)); // boolean must be not null
			else if (fieldType == Date.class)
//				val = c.getString(colIndex) == null ? null : new Date(c.getLong(colIndex));
				val = c.getString(colIndex) == null ? null : parseDate(c.getString(colIndex));
			else if (fieldType == BigDecimal.class)
				val = c.getString(colIndex) == null ? null : new BigDecimal(c.getString(colIndex));
			else
			if (fieldType == Double.class
			|| fieldType == double.class)
				val = c.getDouble(colIndex);
			else if (fieldType == byte[].class)
				val = c.getBlob(colIndex);
			else
				throw new RuntimeException("Unavailable Field Type : " + fieldName + " = " + fieldType);

			set(t, f, val);
		}
		return t;
	}

	private static final String PATTERN = "yyyyMMddHHmmssSSS";

	private static Date parseDate(String date) {
		try {
			return new SimpleDateFormat(PATTERN).parse(date);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	private static String formatDate(Date date) {
		return new SimpleDateFormat(PATTERN).format(date);
	}

	private static ContentValues convert(Table tbl) {
		ContentValues values = new ContentValues();
		for (Field f : tbl.getClass().getFields()) {
			if (Modifier.isStatic(f.getModifiers()))
				continue;
			String fieldName = f.getName();
			if (fieldName.endsWith("_"))
				continue;
			if ("_id".equals(fieldName))
				continue;
			Class<?> fieldType = f.getType();
			String colName = unCamelize(fieldName);
			Object val = get(tbl, f);
			if (fieldType == Integer.class
			|| fieldType == int.class)
				values.put(colName, (Integer)val);
			else if (fieldType == Long.class
			|| fieldType == long.class)
				values.put(colName, (Long)val);
			else if (fieldType == String.class)
				values.put(colName, (String)val);
			else if (fieldType == Date.class)
//				values.put(colName, ((Date)val).getTime());
				values.put(colName, formatDate((Date)val));
			else if (fieldType == boolean.class)
				values.put(colName, val == null ? null : ((Boolean)val).toString());
			else if (fieldType == BigDecimal.class)
				values.put(colName, val == null ? null : ((BigDecimal)val).toString());
			else if (fieldType == Double.class
			|| fieldType == double.class)
				values.put(colName, (Double)val);
			else if (fieldType == byte[].class)
				values.put(colName, (byte[])val);
			else
				throw new RuntimeException("Unavailable Field Type : " + fieldName + " = " + fieldType);

		}
		return values;
	}



	/* ***********************************************************************************/
	/* low level methods */

	public interface IListCallback<T> {
		T process(Cursor c);
	}

//	public static <T> List<T> list(IListCallback<T> callback, String sql, Object...params) {
//		SQLiteDatabase db = DBAdapter.getDB();
//		List<T> ret = list(db, callback, sql, params);
//		DBAdapter.close();
//		return ret;
//	}

	public static <T> List<T> list(SQLiteDatabase db, IListCallback<T> callback, String sql, Object...args) {
//		Log.d(LOG_TAG, "select-sql:[" + sql + "] params:" + Arrays.toString(args));
		ArrayList<T> ret = new ArrayList<T>();
		String[] params = new String[args.length];
		for (int i = 0; i < args.length; i++) {
			params[i] = args[i].toString();
		}
		Cursor c = db.rawQuery(sql, params);
		if (c.moveToFirst()) {
			do {
				ret.add(callback.process(c));
			} while(c.moveToNext());
		}
		c.close();
		return ret;
	}

	public static int count(SQLiteDatabase db, String sql, String...params) {
		int ret = 0;
		Cursor c = db.rawQuery(sql, params);
		if (c.moveToFirst()) {
			ret = c.getInt(0);
		}
		c.close();
		return ret;
	}

	public static long insert(SQLiteDatabase db, String tableName, ContentValues values) {
		Log.d(LOG_TAG, "insert [ " + tableName + "] " + values.toString());
		long rowId = db.insertOrThrow(tableName, null, values);
		Log.d(LOG_TAG, "insert return : " + rowId);
		return rowId;
	}

	public static int update(SQLiteDatabase db, String tableName, ContentValues values, String where, String...params) {
		Log.d(LOG_TAG, "update [ " + tableName + "] " + values.toString());
		int ret = db.update(tableName, values, where, params);
		Log.d(LOG_TAG, "update return : " + ret);
		return ret;
	}

	public static int delete(SQLiteDatabase db, String tableName, String where, String...params) {
		Log.d(LOG_TAG, "delete [ " + tableName + "]");
		int ret = db.delete(tableName, where, params);
		Log.d(LOG_TAG, "delete return : " + ret);
		return ret;
	}


	/* ***********************************************************************************/
	/* private method */

	private static String unCamelize(String camelCase) {
		StringBuffer sb = new StringBuffer();
		sb.append(camelCase.charAt(0));
		for (int i = 1; i < camelCase.length(); i++) {
			char c = camelCase.charAt(i);
			if (c >= 'A' && c <= 'Z') {
				sb.append('_').append(c);
			} else {
				sb.append(c);
			}
		}
		return sb.toString().toUpperCase();
	}

//	private static String camelize(String underbarSplitted) {
//		StringBuffer sb = new StringBuffer();
//		String[] str = underbarSplitted.split("_");
//		for(String temp : str) {
//			sb.append(Character.toUpperCase(temp.charAt(0)));
//			sb.append(temp.substring(1).toLowerCase());
//		}
//		return sb.toString();
//	}


	private static Field getField(Class<?> clazz, String fieldName) {
		try {
			return clazz.getField(fieldName);
		} catch (SecurityException e) {
			throw new RuntimeException("unexpected!", e);
		} catch (NoSuchFieldException e) {
			throw new RuntimeException("unexpected!", e);
		}
	}

//	private static boolean hasField(Class<?> clazz, String fieldName) {
//		try {
//			clazz.getField(fieldName);
//			return true;
//		} catch (SecurityException e) {
//		} catch (NoSuchFieldException e) {
//		}
//		return false;
//	}

	private static void set(Object obj, Field f, Object value) {
		try {
			f.set(obj, value);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException("unexpected!", e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("unexpected!", e);
		}
	}

	private static Object get(Object obj, Field f) {
		try {
			return f.get(obj);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException("unexpected!", e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("unexpected!", e);
		}
	}

	private static <T> T newInstance(Class<T> clazz) {
		try {
			return clazz.newInstance();
		} catch (IllegalAccessException e) {
			throw new RuntimeException("please ready default-constructor for " + clazz.getCanonicalName(), e);
		} catch (InstantiationException e) {
			throw new RuntimeException("unexpected!", e);
		}
	}
}
