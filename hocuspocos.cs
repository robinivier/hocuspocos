/* Hocus Pocos - Turns your pocos into living database objects.
 * Version 0.9
 * 
 * Features
 * 
 * * Single file
 * * MSSQL 2005 +
 * * Clean pocos with no attributes
 * * Fluent API for mapping configuration
 * * Database creation with ability to create tables and relationships
 * * Support for complex type mappings
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Data;
using System.Text;
using System.Reflection;
using System.Collections;
using System.Data.Common;

namespace HocusPocos
{

    public class Db : IDisposable
    {

        private readonly string _connectionString;
        private readonly DbType _dbType;
        private readonly DbProviderFactory _factory;
        private IDbConnection _connection;
        private IDbTransaction _transaction;
        private readonly IQueryBuilder _qb;

        public Db(string connectionString, DbType type)
        {

            _connectionString = connectionString;
            _dbType = type;

            switch (_dbType)
            {
                case DbType.MsSql:
                    _qb = new MsSqlQueryBuilder();
                    break;
            }

            _factory = DbProviderFactories.GetFactory(GetProviderByType(type));

            OpenConnection();

        }

        private static string GetProviderByType(DbType type)
        {
            switch (type)
            {
                case DbType.MsSql:
                    return "System.Data.SqlClient";
                default:
                    return null;
            }
        }

        public enum DbType
        {
            MsSql
        }

        private void OpenConnection()
        {
            if (_connection == null)
            {
                _connection = _factory.CreateConnection();
                if (_connection != null)
                {
                    _connection.ConnectionString = _connectionString;
                    _connection.Open();
                }
            }
        }

        private void CloseConnection()
        {

            if (_connection != null)
            {
                _connection.Close();
                _connection.Dispose();
                _connection = null;
            }

        }

        private void BeginTransaction()
        {
            if (_transaction == null)
            {
                _transaction = _connection.BeginTransaction();
            }
        }

        #region Sql

        public SqlResult<T> Sql<T>(string sql, params object[] args)
        {

            sql = string.Format(sql, args);

            return null;

        }

        #endregion

        #region Load<T>

        /// <summary>
        /// Loads a poco by the primary key id
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <returns></returns>
        public T Load<T>(object id)
        {

            var info = new PocoInfo(typeof(T));

            var query = string.Format("([{0}] = {1})", info.TableInfo.PrimaryKey, Helper.GetSqlValue(id));

            var result = Query(info, query, 0, 0, false).FirstOrDefault();

            if (result != null)
                return (T)result;

            return default(T);

        }

        #endregion

        #region List<T>

        public List<T> List<T>()
        {

            var info = new PocoInfo(typeof(T));

            var result = Query(info, null, 0, 0, false);

            if (result != null && result.Any())
                return result.Cast<T>().ToList();

            return null;

        }

        #endregion

        #region Query<T>

        private object ReaderToPoco(IDataReader reader, PocoInfo info)
        {

            var poco = Activator.CreateInstance(info.Type);

            // Set values for non nested types
            for (var i = 0; i < reader.FieldCount; i++)
            {

                var columnName = reader.GetName(i);

                if (info.Properties.ContainsKey(columnName))
                {

                    var property = info.Properties[columnName];
                    property.SetValue(poco, reader[i]);

                }

            }

            // Handle nested types (with recursion)
            foreach (var property in info.SubProperties)
            {

                // Get IList type
                var listType = Helper.GetIListType(property.Value.Type.PropertyType);

                if (listType != null)
                {

                    // One-to-many

                    var results = Query(property.Value.Info,
                                         String.Format("({0} = '{1}')", info.CompositeKeyName,
                                                       reader[info.TableInfo.PrimaryKey]),
                                                       0,
                                                       0,
                                                       false);

                    if (results != null)
                    {

                        var list = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(listType));

                        foreach (var result in results)
                            list.Add(result);

                        property.Value.SetValue(poco, list);

                    }

                }
                else
                {

                    // One-to-one

                    object result = null;
                    // Check if the current poco has the nested poco compositekey
                    if (info.Properties.ContainsKey(property.Key + property.Value.Info.CompositeKeyName))
                    {

                        if (info.Properties[property.Key + property.Value.Info.CompositeKeyName].GetValue(poco) != null)
                        {

                            result = Query(property.Value.Info,
                                String.Format("({0} = '{1}')", property.Value.Info.TableInfo.PrimaryKey,
                                                              reader[property.Key + property.Value.Info.CompositeKeyName]),
                                                              0,
                                                              1,
                                                              false).FirstOrDefault();

                        }

                    }
                    else if (info.Properties.ContainsKey(property.Value.Info.CompositeKeyName))
                    {

                        if (info.Properties[property.Value.Info.CompositeKeyName].GetValue(poco) != null)
                        {

                            result = Query(property.Value.Info,
                                String.Format("({0} = '{1}')", property.Value.Info.TableInfo.PrimaryKey,
                                                              reader[property.Value.Info.CompositeKeyName]),
                                                              0,
                                                              1,
                                                              false).FirstOrDefault();

                        }

                    }
                    else
                    {

                        // If not...

                        result = Query(property.Value.Info,
                                                    String.Format("({0} = '{1}')", info.TableInfo.PrimaryKey != null ? info.CompositeKeyName : property.Value.Info.TableInfo.PrimaryKey,
                                                                                  reader[info.TableInfo.PrimaryKey ?? property.Value.Info.CompositeKeyName]),
                                                                                  0,
                                                                                  1,
                                                                                  false).FirstOrDefault();

                    }

                    if (result != null)
                        property.Value.SetValue(poco, result);

                }

            }

            return poco;

        }

        private IEnumerable<object> Query(PocoInfo info, string query, int skip, int take, bool distinct)
        {

            var sql = _qb.SelectQuery(info, query, skip, take, distinct);

            //Barracoda.Models.Interfaces.ILogger logger = new Barracoda.Helpers.Logger();

            //logger.Info("RUNNING SQL -> " + sql);

            using (var command = CreateCommand(sql, _connection, _transaction))
            {

                using (var reader = command.ExecuteReader())
                {

                    while (true)
                    {

                        if (!reader.Read())
                            yield break;

                        yield return ReaderToPoco(reader, info);

                    }

                }

            }
        }

        private IEnumerable<T> Query<T>(string query, int skip, int take, bool distinct)
        {

            return Query(new PocoInfo(typeof(T)),
                query,
                skip,
                take,
                distinct).Cast<T>();

        }

        /// <summary>
        /// Queries the db for the specified poco
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public QueryResult<T> Query<T>()
        {

            return new QueryResult<T>(this);

        }

        #endregion

        #region Add<T>

        /// <summary>
        /// Adds a poco to the db
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="poco"></param>
        /// <returns></returns>
        public T Add<T>(T poco)
        {

            BeginTransaction();

            var info = new PocoInfo(poco.GetType());

            Add(info, poco);

            return poco;

        }

        private void Add(PocoInfo info, object poco, string key = null, object id = null)
        {

            var sql = _qb.InsertQuery(info, poco, key, id);

            id = ExecuteScalar(sql);

            if (info.TableInfo.PrimaryKey != null)
                info.Properties[info.TableInfo.PrimaryKey].SetValue(poco, id);

            foreach (var property in info.SubProperties)
            {

                var value = property.Value.GetValue(poco);

                if (value != null)
                {

                    // Get IList type
                    var listType = Helper.GetIListType(property.Value.Type.PropertyType);

                    if (listType != null)
                    {

                        var list = (IList)value;

                        foreach (var item in list)
                            Add(property.Value.Info, item, info.CompositeKeyName, id);

                    }
                    else
                    {
                        Add(property.Value.Info, value, info.CompositeKeyName, id);
                    }

                }

            }

        }

        #endregion

        #region Remove<T>

        /// <summary>
        /// Removes a poco recursivly
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="poco"></param>
        /// <returns></returns>
        public int RemoveAll<T>(T poco)
        {

            BeginTransaction();

            var info = new PocoInfo(typeof(T));

            return Remove(info, poco);

        }

        private int Remove(PocoInfo info, object poco)
        {

            foreach (var property in info.SubProperties)
            {

                var value = property.Value.GetValue(poco);

                if (value != null)
                {

                    // Get IList type
                    var listType = Helper.GetIListType(property.Value.Type.PropertyType);

                    if (listType != null)
                    {

                        var list = (IList)value;

                        foreach (var item in list)
                            Remove(property.Value.Info, item);

                    }
                    else
                    {
                        Remove(property.Value.Info, value);
                    }

                }

            }

            var sql = _qb.DeleteQuery(info, poco);

            return ExecuteNonQuery(sql);

        }

        /// <summary>
        /// Remove a poco by primary key id
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <returns></returns>
        public int Remove<T>(object id)
        {

            var info = new PocoInfo(typeof(T));

            var sql = String.Format("DELETE FROM {0} WHERE {1} = '{2}'",
                info.TableInfo.Name,
                info.TableInfo.PrimaryKey,
                id);

            return ExecuteNonQuery(sql);

        }

        /// <summary>
        /// Removes pocos by expression
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expression"></param>
        /// <returns></returns>
        public int Remove<T>(Expression<Func<T, bool>> expression)
        {

            var info = new PocoInfo(typeof(T));

            ILambdaSerializer lambdaToSql = null;

            switch (_dbType)
            {
                case DbType.MsSql:
                    lambdaToSql = new MsSqlLambdaSerializer(typeof(T));
                    break;
            }

            var sql = String.Format("DELETE FROM {0} WHERE {1} ",
                info.TableInfo.Name,
                lambdaToSql.ToSql(expression));

            return ExecuteNonQuery(sql);

        }

        #endregion

        #region Edit<T>

        private object Edit(PocoInfo info, object poco, string key = null, object id = null)
        {

            if (info.TableInfo.PrimaryKey != null && key == null)
                id = info.Properties[info.TableInfo.PrimaryKey].GetValue(poco);

            foreach (var property in info.SubProperties)
            {

                var value = property.Value.GetValue(poco);

                if (value != null)
                {

                    // Get IList type
                    var listType = Helper.GetIListType(property.Value.Type.PropertyType);

                    if (listType != null)
                    {

                        var list = (IList)value;

                        foreach (var item in list)
                        {

                            if (property.Value.Info.TableInfo.PrimaryKey != null)
                            {

                                var primaryKey = property.Value.Info.Properties[property.Value.Info.TableInfo.PrimaryKey];

                                var defaultValue = Helper.GetDefaultValue(primaryKey.Type.PropertyType);

                                if (primaryKey.GetValue(item).Equals(defaultValue))
                                {
                                    Add(property.Value.Info, item, info.CompositeKeyName, id);
                                }
                                else
                                {
                                    Edit(property.Value.Info, item, info.CompositeKeyName, id);
                                }

                            }
                            else
                            {
                                Edit(property.Value.Info, item, info.CompositeKeyName, id);
                            }

                        }
                    }
                    else
                    {

                        // Child has parent key
                        if (info.CompositeKeyName != null && property.Value.Info.Properties.ContainsKey(info.CompositeKeyName))
                            Edit(property.Value.Info, value, info.CompositeKeyName, id);
                        // Parent has child key
                        else if (property.Value.Info.CompositeKeyName != null && info.Properties.ContainsKey(property.Value.Info.CompositeKeyName))
                            info.Properties[property.Value.Info.CompositeKeyName].SetValue(poco,
                                Edit(property.Value.Info, value, null, null));

                    }

                }

            }

            string sql;

            if (id != null)
                sql = _qb.UpdateQuery(info, poco, key, id);
            else
                sql = _qb.InsertQuery(info, poco);

            var result = ExecuteScalar(sql);

            if (info.TableInfo.PrimaryKey != null)
                info.Properties[info.TableInfo.PrimaryKey].SetValue(poco, result);

            return result;

        }

        public void Edit<T>(T poco)
        {

            BeginTransaction();

            var info = new PocoInfo(typeof(T));

            Edit(info, poco);

        }

        #endregion

        private int Count<T>(string query)
        {

            var info = new PocoInfo(typeof(T));

            var sql = string.Format("SELECT COUNT(0) FROM {0}",
                                    info.TableInfo.Name);

            sql += !String.IsNullOrWhiteSpace(query) ? " WHERE " + query : "";

            return (int)ExecuteScalar(sql, false);

        }

        private bool Exists<T>(string query)
        {

            return Count<T>(query) > 0;

        }

        private static IDbCommand CreateCommand(string sql, IDbConnection connection, IDbTransaction transaction)
        {

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.CommandText = sql;
            command.Transaction = transaction;

            return command;

        }

        public object ExecuteScalar(string sql, bool returnIdentity = true)
        {
            try
            {

                // Get id?
                if (returnIdentity)
                {
                    switch (_dbType)
                    {
                        case DbType.MsSql:
                            sql += ";SELECT scope_identity()";
                            break;
                    }
                }

                using (var command = CreateCommand(sql, _connection, _transaction))
                {
                    return command.ExecuteScalar();
                }
            }
            catch
            {

                if (_transaction != null)
                {
                    _transaction.Rollback();
                    _transaction.Dispose();
                    _transaction = null;
                }

            }

            return null;

        }

        private bool TableExists(string name)
        {

            try
            {

                var sql = string.Format("SELECT 1 FROM {0} WHERE 1 = 2", name);

                using (var command = CreateCommand(sql, _connection, _transaction))
                {
                    command.ExecuteScalar();
                    return true;
                }

            }
            catch
            {
                return false;
            }

        }

        public int ExecuteNonQuery(string sql)
        {

            try
            {

                using (var command = CreateCommand(sql, _connection, _transaction))
                {

                    return command.ExecuteNonQuery();

                }

            }
            catch
            {

                if (_transaction != null)
                {
                    _transaction.Rollback();
                    _transaction.Dispose();
                    _transaction = null;
                }

            }

            return 0;

        }

        /// <summary>
        /// Save a poco to the db
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="poco"></param>
        public void Save<T>(T poco)
        {

            var info = new PocoInfo(typeof(T));

            if (info.TableInfo.PrimaryKey != null)
            {

                BeginTransaction();

                var primaryKey = info.Properties[info.TableInfo.PrimaryKey];

                var defaultValue = Helper.GetDefaultValue(primaryKey.Type.PropertyType);

                // Insert or update?
                if (primaryKey.GetValue(poco).Equals(defaultValue))
                {
                    Add(info, poco);
                }
                else
                {
                    Edit(info, poco);
                }

                CompleteTransaction();

            }

        }

        private void CompleteTransaction()
        {

            if (_transaction != null)
            {
                _transaction.Commit();
                _transaction = null;
            }

        }

        /// <summary>
        /// Commits all open transactions to the db
        /// </summary>
        public void Save()
        {
            CompleteTransaction();
        }

        /// <summary>
        /// Rollback any uncommited transactions and closes the db connection
        /// </summary>
        public void Dispose()
        {
            if (_transaction != null)
            {
                _transaction.Rollback();
                _transaction = null;
            }
            CloseConnection();
        }

        public class SqlResult<T> : IQueryable<T>
        {

            public IEnumerator<T> GetEnumerator()
            {
                throw new NotImplementedException();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                throw new NotImplementedException();
            }

            public Type ElementType
            {
                get { throw new NotImplementedException(); }
            }

            public Expression Expression
            {
                get { throw new NotImplementedException(); }
            }

            public IQueryProvider Provider
            {
                get { throw new NotImplementedException(); }
            }

        }

        /// <summary>
        /// The query result object
        /// </summary>
        /// <typeparam name="T"></typeparam>
        public class QueryResult<T> : IQueryable<T>, IQueryProvider
        {

            private readonly Db _db;
            public string Query { get; set; }
            public string OrderBy { get; set; }
            private Type _type = typeof(T);
            private int _skip;
            private int _take;
            private bool _distinct;
            private ILambdaSerializer _lambdaSerializer;

            public int Count
            {
                get
                {
                    return _db.Count<T>(Query);
                }
            }

            public QueryResult(Db db)
            {
                _db = db;

                switch (_db._dbType)
                {
                    case DbType.MsSql:
                        _lambdaSerializer = new MsSqlLambdaSerializer(_type);
                        return;
                }

            }

            #region Query

            public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
            {

                var call = expression as MethodCallExpression;

                if (call != null)
                {

                    switch (call.Method.Name)
                    {
                        case "Where":
                            Query = _lambdaSerializer.ToSql((((UnaryExpression)call.Arguments[1]).Operand as LambdaExpression));
                            break;
                        case "Skip":
                            _skip = (int)((ConstantExpression)call.Arguments[1]).Value;
                            break;
                        case "Take":
                            _take = (int)((ConstantExpression)call.Arguments[1]).Value;
                            break;
                        case "Distinct":
                            _distinct = true;
                            break;
                        case "OrderBy":
                            throw new NotImplementedException("OrderBy");
                            break;
                    }

                }

                return (IQueryable<TElement>)this;

            }

            public IQueryable CreateQuery(Expression expression)
            {
                return CreateQuery<T>(expression);
            }

            #endregion

            #region Execute

            public TResult Execute<TResult>(Expression expression)
            {

                var call = expression as MethodCallExpression;

                if (call != null)
                {

                    switch (call.Method.Name)
                    {
                        case "Any":
                            throw new NotImplementedException("Any");
                        case "Count":
                            throw new NotImplementedException("Count");
                        case "First":
                            throw new NotImplementedException("First");
                        case "FirstOrDefault":
                            if (call.Arguments.Count > 1)
                                Query = _lambdaSerializer.ToSql(((UnaryExpression)call.Arguments[1]).Operand as LambdaExpression);
                            return _db.Query<TResult>(Query, _skip, 1, false).FirstOrDefault();

                    }

                }

                return default(TResult);

            }

            public object Execute(System.Linq.Expressions.Expression expression)
            {
                return Execute<T>(expression);
            }

            #endregion

            #region Enumerator

            public IEnumerator<T> GetEnumerator()
            {

                return GetResults();

            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private IEnumerator<T> GetResults()
            {

                IEnumerable<T> results;

                results = _db.Query<T>(Query, _skip, _take, _distinct);

                return results.GetEnumerator();

            }

            #endregion

            public Type ElementType
            {
                get { return typeof(T); }
            }

            public Expression Expression
            {
                get { return Expression.Constant(this); }
            }

            public IQueryProvider Provider
            {
                get { return this; }
            }

        }

        #region Lambda Serializer

        private class MsSqlLambdaSerializer : ILambdaSerializer
        {

            private Type _type;

            public MsSqlLambdaSerializer(Type type)
            {
                _type = type;
            }

            public string ToSql(LambdaExpression e)
            {

                var sb = new StringBuilder();

                ParseQuery(e.Body, sb);

                return sb.ToString();

            }

            #region Helpers

            private void ParseQuery(Expression e, StringBuilder sb)
            {

                sb.Append("(");

                if (e is BinaryExpression)
                {
                    var c = e as BinaryExpression;
                    switch (c.NodeType)
                    {
                        case ExpressionType.AndAlso:
                            ParseQuery(c.Left, sb);
                            sb.Append(" AND ");
                            ParseQuery(c.Right, sb);
                            break;
                        case ExpressionType.OrElse:
                            ParseQuery(c.Left, sb);
                            sb.Append(" OR ");
                            ParseQuery(c.Right, sb);
                            break;
                        default: // Equal, NotEqual, GreaterThan
                            sb.Append(GetCondition(c));
                            break;
                    }
                }
                else if (e is UnaryExpression)
                {
                    var c = e as UnaryExpression;

                    switch (c.NodeType)
                    {
                        case ExpressionType.Not:
                            sb.Append(" NOT ");
                            ParseQuery(c.Operand, sb);
                            break;
                        default:
                            throw new NotSupportedException("Not supported: " + c.NodeType);
                    }

                }

                else if (e is MethodCallExpression)
                {
                    var m = e as MethodCallExpression;
                    var o = (m.Object as MemberExpression);
                    if (m.Method.DeclaringType == typeof(string))
                    {
                        switch (m.Method.Name)
                        {
                            case "Contains":
                                {
                                    //var c = m.Arguments[0] as ConstantExpression;
                                    var v = Expression.Lambda(m.Arguments[0]).Compile().DynamicInvoke().ToString();
                                    sb.AppendFormat("{0} LIKE '%{1}%'", o.Member.Name, v);
                                    break;
                                }
                            case "StartsWith":
                                {
                                    //var c = m.Arguments[0] as ConstantExpression;
                                    var v = Expression.Lambda(m.Arguments[0]).Compile().DynamicInvoke().ToString();
                                    sb.AppendFormat("{0} LIKE '{1}%'", o.Member.Name, v);
                                    break;
                                }
                            case "EndsWith":
                                {
                                    var v = Expression.Lambda(m.Arguments[0]).Compile().DynamicInvoke().ToString();
                                    sb.AppendFormat("{0} LIKE '%{1}'", o.Member.Name, v);
                                    break;
                                }
                            default:
                                throw new NotSupportedException();
                        }
                    }
                    else
                        throw new NotSupportedException();
                }
                else
                    throw new NotSupportedException();

                sb.Append(")");

            }

            private string GetCondition(BinaryExpression e)
            {

                string field;
                object value;

                if (e.Left is MemberExpression && ((MemberExpression)e.Left).Member.DeclaringType == _type)
                {
                    field = ((MemberExpression)e.Left).Member.Name;
                    value = Expression.Lambda(e.Right).Compile().DynamicInvoke();
                }
                else if (e.Right is MemberExpression && ((MemberExpression)e.Right).Member.DeclaringType == _type)
                {
                    field = ((MemberExpression)e.Right).Member.Name;
                    value = Expression.Lambda(e.Left).Compile().DynamicInvoke().ToString();
                }
                else
                    throw new NotSupportedException();

                switch (e.NodeType)
                {
                    case ExpressionType.Equal:
                        if (value == null)
                            return String.Format("{0} IS NULL", field);
                        else
                            return String.Format("{0} = {1}", field, Helper.GetSqlValue(value));
                    case ExpressionType.NotEqual:
                        if (value == null)
                            return String.Format("({0} IS NOT NULL)", field);
                        else
                            return String.Format("({0} <> {1})", field, Helper.GetSqlValue(value));
                    default:
                        throw new NotSupportedException("Not supported: " + e.NodeType);

                }

            }

            #endregion

        }

        private interface ILambdaSerializer
        {

            string ToSql(LambdaExpression e);
            //string ToOrderByClause(LabelExpression e);

        }

        #endregion

        #region Query Builder

        private class MsSqlQueryBuilder : IQueryBuilder
        {

            public string SelectQuery(PocoInfo info, string query, int skip, int take, bool distinct)
            {

                List<string> sorting = null;

                if (info.TableInfo.Sorting != null)
                {
                    sorting = new List<string>();

                    foreach (var item in info.TableInfo.Sorting)
                        sorting.Add(string.Format("[{0}] {1}", item.Key, item.Value));

                }

                var sql = String.Format(@"SELECT {0} {1} *
                                            FROM (
                                              (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS ROWNUM,
                	                            *
                	                            FROM [{2}]
                                                {3})
                                            ) AS t1 {4} {5}",
                                            distinct ? " DISTINCT " : "",
                                            take > 0 ? string.Format(" TOP {0} ", take) : "",
                                            info.TableInfo.Name,
                                            !string.IsNullOrWhiteSpace(query) ? " WHERE " + query : "",
                                            skip > 0 ? string.Format(" WHERE (t1.ROWNUM > {0}) ", skip) : "",
                                            sorting != null ? "ORDER BY " + string.Join(",", sorting) : "");

                return sql;

            }

            public string InsertQuery(PocoInfo info, object poco, string key = null, object id = null)
            {

                var columns = new List<string>();
                var values = new List<string>();

                foreach (var property in info.Properties)
                {

                    if (property.Value.IsVirtual)
                        continue;

                    // Can insert guid even if is primary key
                    if (property.Value.IsPrimaryKey)
                    {

                        if (property.Value.Type.PropertyType == typeof(Guid))
                        {
                            property.Value.SetValue(poco, Guid.NewGuid());
                        }
                        else
                        {
                            continue;
                        }

                    }

                    columns.Add("[" + property.Value.ColumnName + "]");

                    // Value
                    if (property.Value.ColumnName == key)
                        property.Value.SetValue(poco, id);

                    values.Add(Helper.GetSqlValue(property.Value.GetValue(poco), property.Value.IsNullableType));

                }

                var sql = string.Format("INSERT INTO [{0}] ({1}) VALUES ({2})",
                                   info.TableInfo.Name,
                                   string.Join(", ", columns),
                                   string.Join(", ", values));

                return sql;

            }

            public string DeleteQuery(PocoInfo info, object poco)
            {

                var sql = String.Format("DELETE FROM [{0}] WHERE [{1}] = {2}",
                                        info.TableInfo.Name,
                                        info.TableInfo.PrimaryKey,
                                        info.Properties[info.TableInfo.PrimaryKey].GetValue(poco));

                return sql;

            }

            public string UpdateQuery(PocoInfo info, object poco, string key = null, object id = null)
            {

                var rows = new List<string>();

                foreach (var property in info.Properties)
                {

                    if (!property.Value.IsPrimaryKey)
                    {

                        rows.Add(String.Format("[{0}] = {1}",
                            property.Value.ColumnName,
                            Helper.GetSqlValue(property.Value.GetValue(poco))));

                    }

                }

                var sql = String.Format("UPDATE [{0}] SET {1}",
                    info.TableInfo.Name,
                    String.Join(", ", rows));

                sql += WhereStatement(info, poco, key, id);

                return sql;

            }

            public string WhereStatement(PocoInfo info, object poco, string key = null, object id = null)
            {

                var where = new List<string>();

                if (key != null)
                    where.Add(string.Format("[{0}] = {1}", key, Helper.GetSqlValue(id)));

                foreach (var name in info.TableInfo.PrimaryKeys)
                {
                    where.Add(string.Format("({0} = {1})", name, Helper.GetSqlValue(info.Properties[name].GetValue(poco))));
                }

                return " WHERE (" + string.Join(" AND ", where) + ")";

            }

        }

        private interface IQueryBuilder
        {

            string SelectQuery(PocoInfo info, string query, int skip, int take, bool distinct);
            string InsertQuery(PocoInfo info, object poco, string key = null, object id = null);
            string DeleteQuery(PocoInfo info, object poco);
            string UpdateQuery(PocoInfo info, object poco, string key = null, object id = null);
            string WhereStatement(PocoInfo info, object poco, string key = null, object id = null);

        }

        #endregion

        #region Db Creator

        public IDbCreator DatabaseCreator(DbCreatorOptions options)
        {

            switch (_dbType)
            {
                case DbType.MsSql:
                    return new MsSqlDbCreator(this, options);
            }

            return null;

        }

        /// <summary>
        /// Db creator for MsSql Server database
        /// </summary>
        public class MsSqlDbCreator : IDbCreator
        {

            private string _script;
            private readonly Db _db;
            private readonly DbCreatorOptions _options;
            private readonly Dictionary<string, PocoInfo> _queue = new Dictionary<string, PocoInfo>();

            public MsSqlDbCreator(Db db, DbCreatorOptions options)
            {

                _options = options;

                _db = db;

            }

            public string Script
            {
                get
                {

                    // Script tables
                    if (_options.HasFlag(DbCreatorOptions.Tables))
                    {

                        foreach (var item in _queue)
                            CreateTableWithOptions(item.Value);

                    }

                    return _script;

                }
                set { _script = value; }
            }

            public IDbCreator CreateTable<T>()
            {

                var info = new PocoInfo(typeof(T));

                if (!_db.TableExists(info.TableInfo.Name))
                    _queue.Add(info.Type.Name, info);

                return this;

            }

            public void Execute()
            {

                if (Script != null)
                    _db.ExecuteNonQuery(_script.Replace("\r\n", " "));

            }

            private void CreateTableWithOptions(PocoInfo info)
            {

                var script = new StringBuilder();

                script.Append(string.Format("CREATE TABLE [{0}](", info.TableInfo.Name));

                var columns = new List<string>();

                foreach (var property in info.Properties)
                {

                    var name = property.Value.ColumnName;

                    if (info.TableInfo.ColumnMappings != null)
                    {

                        var keyValuePair = info.TableInfo.ColumnMappings.FirstOrDefault(x => x.Value == property.Key);

                        if (keyValuePair.Key != null)
                            name = keyValuePair.Key;

                    }

                    var column = string.Format("\r\n[{0}]", name);

                    // Sql db type
                    column += string.Format(" {0}", Helper.GetSqlDbType(property.Value.Type));

                    // Identity
                    if (property.Value.IsPrimaryKey && info.TableInfo.IdentityIncrement)
                    {
                        column += " IDENTITY(1,1)";
                    }

                    // Nullable
                    column += property.Value.IsNullableType ? " NULL" : " NOT NULL";

                    columns.Add(column);

                }

                // Append columns
                script.Append(string.Join(",", columns));

                // Primary key constraint
                if (info.TableInfo.PrimaryKeys != null && info.TableInfo.PrimaryKeys.Any())
                {

                    script.Append(string.Format("\r\n, CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED (", info.TableInfo.Name));

                    var keys = new List<string>();

                    foreach (var primaryKey in info.TableInfo.PrimaryKeys)
                        keys.Add(string.Format("\r\n[{0}] ASC", primaryKey));

                    // Append keys
                    script.Append(string.Join(",", keys));

                    script.Append(
                        "\r\n) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]");

                }

                script.Append("\r\n) ON [PRIMARY]");

                // Go
                script.Append("\r\n;\r\n");

                // Prepend create table to script
                _script = script.ToString() + _script;

                //// Loop through nested properties and add foreign keys
                //foreach (var property in info.SubProperties)
                //{

                //    if (info.CompositeKeyName != null)
                //    {

                //        // Append alter table to script

                //        _script += string.Format("ALTER TABLE [{0}] WITH CHECK ADD CONSTRAINT [FK_{0}_{1}] FOREIGN KEY([{2}]) REFERENCES [{1}] ([{3}])\r\n;\r\n",
                //           property.Value.Info.TableInfo.Name,
                //           info.TableInfo.Name,
                //           info.CompositeKeyName,
                //           info.TableInfo.PrimaryKey);

                //        _script += string.Format("ALTER TABLE [{0}] CHECK CONSTRAINT [FK_{0}_{1}]\r\n;\r\n",
                //                                    property.Value.Info.TableInfo.Name,
                //                                    info.TableInfo.Name);

                //    }

                //}

                //// Add constraints from foreign keys
                ////if (info.CompositeKeyName != null && info.TableInfo.ForeignKeys != null && info.TableInfo.ForeignKeys.Any())
                //if (info.TableInfo.ForeignKeys != null && info.TableInfo.ForeignKeys.Any())
                //{

                //    int i = 0;

                //    foreach (var property in info.TableInfo.ForeignKeys)
                //    {

                //        // Append alter table to script

                //        _script += string.Format("ALTER TABLE [{0}] WITH CHECK ADD CONSTRAINT [FK_{0}_{1}_{4}] FOREIGN KEY([{2}]) REFERENCES [{1}] ([{3}])\r\n;\r\n",
                //           info.TableInfo.Name,
                //           property.Value.Name,
                //           property.Key,
                //           property.Value.PrimaryKey,
                //           i);

                //        _script += string.Format("ALTER TABLE [{0}] CHECK CONSTRAINT [FK_{0}_{1}_{2}]\r\n;\r\n",
                //                                    info.TableInfo.Name,
                //                                    property.Value.Name,
                //                                    i);

                //        i += 1;

                //    }

                //}

            }

        }

        public interface IDbCreator
        {

            string Script { get; }

            IDbCreator CreateTable<T>();
            void Execute();

        }

        public enum DbCreatorOptions
        {
            Tables
        }

        #endregion

    }

    internal class Helper
    {

        //internal static T ClonePoco<T>(T poco)
        //{

        //    if (poco == null)
        //        return default(T);

        //    var info = new PocoInfo(poco.GetType());

        //    T clone = Activator.CreateInstance<T>();

        //    return (T)ClonePoco(poco, clone, info);

        //}

        //private static object ClonePoco(object source, object clone, PocoInfo info)
        //{

        //    foreach (var property in info.Properties)
        //    {

        //        if (!property.Value.IsPrimaryKey)
        //        {

        //            clone.GetType().GetProperty(property.Key)
        //                .SetValue(clone, property.Value.GetValue(source), null);

        //        }

        //    }

        //    foreach (var property in info.SubProperties)
        //    {

        //        clone.GetType().GetProperty(property.Key)
        //            .SetValue(clone, ClonePoco(property.Value.Type), null);

        //    }

        //    return clone;

        //}

        //public static object CloneObject(object opSource)
        //{
        //    //grab the type and create a new instance of that type
        //    Type opSourceType = opSource.GetType();
        //    object opTarget = Activator.CreateInstance(opSourceType);

        //    //grab the properties
        //    PropertyInfo[] opPropertyInfo = opSourceType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);

        //    //iterate over the properties and if it has a 'set' method assign it from the source TO the target
        //    foreach (PropertyInfo item in opPropertyInfo)
        //    {
        //        if (item.CanWrite)
        //        {
        //            //value types can simply be 'set'
        //            if (item.PropertyType.IsValueType || item.PropertyType.IsEnum || item.PropertyType.Equals(typeof(System.String)))
        //            {
        //                item.SetValue(opTarget, item.GetValue(opSource, null), null);
        //            }
        //            //object/complex types need to recursively call this method until the end of the tree is reached
        //            else
        //            {
        //                object opPropertyValue = item.GetValue(opSource, null);
        //                if (opPropertyValue == null)
        //                {
        //                    item.SetValue(opTarget, null, null);
        //                }
        //                else
        //                {
        //                    item.SetValue(opTarget, CloneObject(opPropertyValue), null);
        //                }
        //            }
        //        }
        //    }
        //    //return the new item
        //    return opTarget;

        //}

        internal static object GetDefaultValue(Type type)
        {

            object output = null;

            if (type.IsValueType)
            {
                output = Activator.CreateInstance(type);
            }

            return output;

        }

        internal static string GetPropertyNameFromExpression<T>(Expression<Func<T, object>> property)
        {

            PropertyInfo propertyInfo = null;

            if (property.Body is MemberExpression)
                propertyInfo = (property.Body as MemberExpression).Member as PropertyInfo;
            else
                propertyInfo = (((UnaryExpression)property.Body).Operand as MemberExpression).Member as PropertyInfo;

            return propertyInfo.Name;

        }

        // TODO: Move?
        internal static string GetSqlValue(object value, bool isNullable = false)
        {
            if (value == null)
                return "null";

            if (IsNumericType(value.GetType()))
                return value.ToString();

            if (IsDate(value.GetType()) && (DateTime)value == DateTime.MinValue)
                return isNullable ? "null" : "'1970-01-01'";

            return "'" + value + "'";

        }

        internal static bool IsDate(Type t)
        {
            if (t == typeof(DateTime))
                return true;
            return false;
        }

        // TODO: Move?
        internal static string GetParentKeyName(Type t)
        {

            foreach (var property in t.GetProperties())
            {

                if (property.Name.ToLower() == "id")
                    return t.Name + property.Name;

                if (property.Name.ToLower() == t.Name.ToLower() + property.Name.ToLower())
                    return t.Name + property.Name;

            }

            return null;

        }

        // TODO: Move?
        internal static List<string> GetPrimaryKeys(Type t)
        {

            if (t == null)
                return null;

            var key = GetPrimaryKeyName(t);

            if (key != null)
                return new List<string> { GetPrimaryKeyName(t) };

            return null;

        }

        internal static bool IsVirtual(PropertyInfo property)
        {

            foreach (MethodInfo info in property.GetAccessors())
            {
                if (info.IsVirtual)
                    return true;
            }

            return false;

        }

        internal static bool IsNumericType(Type t)
        {

            TypeCode tc;
            if (IsNullableType(t) && t.IsValueType)
                tc = Type.GetTypeCode(t.GetGenericArguments()[0]);
            else
                tc = Type.GetTypeCode(t);

            return tc >= TypeCode.SByte && tc <= TypeCode.UInt64;

        }

        internal static bool IsNullableType(Type t)
        {
            if (t == null) return true;
            if (!t.IsValueType) return true;
            if (Nullable.GetUnderlyingType(t) != null) return true;
            return false;
        }

        // TODO: Move?
        internal static string GetTableName(Type t)
        {

            if (t == null)
                return null;

            if (GetIListType(t) != null)
                t = GetIListType(t);

            return t.Name;

        }

        // TODO: Move?
        internal static string GetPrimaryKeyName(Type t)
        {

            if (t == null)
                return null;

            foreach (var property in t.GetProperties())
            {

                if (property.Name.ToLower() == "id")
                    return property.Name;

                if (property.Name.ToLower() == t.Name.ToLower() + property.Name.ToLower())
                    return t.Name + property.Name;

            }

            return null;

        }

        internal static Type GetUnderlyingType(Type t)
        {
            if (t.GetGenericArguments().Any())
                return t.GetGenericArguments()[0];
            return t;
        }

        internal static Type GetIListType(Type t)
        {
            if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IList<>))
                return t.GetGenericArguments()[0];
            return null;
        }

        // TODO: Move?
        internal static string GetSqlDbType(PropertyInfo property)
        {

            var typeName = property.PropertyType.Name;

            if (IsNullableType(property.PropertyType))
                typeName = GetUnderlyingType(property.PropertyType).Name;

            switch (typeName)
            {
                case "Int16":
                    return "tinyint";
                case "Int32":
                    return "int";
                case "Int64":
                    return "bigint";
                case "String":
                    return "varchar(max)";
                case "DateTime":
                    return "datetime";
                case "Guid":
                    return "uniqueidentifier";
                case "Decimal":
                    return "decimal(18, 8)";
                case "Double":
                    return "float";
                case "Boolean":
                    return "bit";
                default:
                    throw new Exception("Property type is not defined.");
            }
        }

    }

    #region Info Classes

    internal class TableInfo
    {

        public string Name { get; set; }
        public List<string> PrimaryKeys { get; set; }
        public bool IdentityIncrement { get; set; }
        public Dictionary<string, string> ColumnMappings { get; private set; }
        public Dictionary<string, TableInfo> ForeignKeys { get; private set; }
        public Dictionary<string, string> Sorting { get; private set; }
        public List<string> Excludes { get; private set; }

        public enum SortDirection
        {
            ASC,
            DESC
        }

        internal void Exclude(string propertyName)
        {

            if (Excludes == null)
                Excludes = new List<string>();

            Excludes.Add(propertyName);

        }

        /// <summary>
        /// Add foreign key
        /// </summary>
        /// <param name="propertyName"></param>
        /// <param name="info">Other table info</param>
        internal void AddForeignKey(string propertyName, TableInfo info)
        {

            if (ForeignKeys == null)
                ForeignKeys = new Dictionary<string, TableInfo>();

            ForeignKeys.Add(propertyName, info);

        }

        internal void AddColumnMapping(string propertyName, string name)
        {

            if (ColumnMappings == null)
                ColumnMappings = new Dictionary<string, string>();

            ColumnMappings.Add(name, propertyName);

        }

        internal void AddSorting(string propertyName, SortDirection direction)
        {

            if (Sorting == null)
                Sorting = new Dictionary<string, string>();

            Sorting.Add(propertyName, direction.ToString());

        }

        public TableInfo(string name, bool identityIncrement)
        {
            Name = name;
            IdentityIncrement = identityIncrement;
        }

        public TableInfo(string name)
        {

            Name = name;

            if (PrimaryKeys != null)
                IdentityIncrement = true;

        }

        /// <summary>
        /// Returns a single primary key
        /// </summary>
        internal string PrimaryKey
        {
            get { return PrimaryKeys != null && PrimaryKeys.Count() == 1 ? PrimaryKeys[0] : null; }
        }

        internal void AddPrimaryKey(string name)
        {

            if (PrimaryKeys == null)
                PrimaryKeys = new List<string>();

            PrimaryKeys.Add(name);

        }

    }

    internal class PocoInfo
    {

        public Type Type { get; set; }
        public TableInfo TableInfo { get; set; }
        public string CompositeKeyName { get; set; }
        public Dictionary<string, PocoPropertyInfo> Properties { get; set; }
        public Dictionary<string, PocoPropertyInfo> SubProperties { get; set; }

        public PocoInfo(Type t, Type parent = null, Type previous = null)
        {

            // Return if recursive
            if (t == previous)
                return;

            Type = t;

            // Try to get table info from mapper first
            TableInfo = Mapper.GetTableInfo(t) ?? new TableInfo(Helper.GetTableName(t), true);

            if (TableInfo.PrimaryKeys == null)
                TableInfo.PrimaryKeys = Helper.GetPrimaryKeys(t);

            if (TableInfo.PrimaryKey != null)
                CompositeKeyName = Helper.GetParentKeyName(t) ?? Helper.GetParentKeyName(parent); // Used for selecting nested types

            Properties = new Dictionary<string, PocoPropertyInfo>();
            SubProperties = new Dictionary<string, PocoPropertyInfo>();

            foreach (var property in t.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {

                if (TableInfo.Excludes != null && TableInfo.Excludes.Contains(property.Name))
                    continue;

                var p = new PocoPropertyInfo()
                {
                    IsPrimaryKey = property.Name == TableInfo.PrimaryKey,
                    ColumnName = Mapper.GetColumnMapping(Type, property.Name) ?? property.Name,
                    IsVirtual = Helper.IsVirtual(property),
                    Type = property,
                    IsNullableType = Helper.IsNullableType(property.PropertyType),
                    IsNumericType = Helper.IsNumericType(property.PropertyType),
                    Info = Helper.IsVirtual(property) ? new PocoInfo(Helper.GetUnderlyingType(property.PropertyType), t, parent) : null
                };

                if (Helper.IsVirtual(property))
                {

                    // TODO: Add foreign keys to TableInfo

                    SubProperties.Add(property.Name, p);

                }
                else
                {
                    Properties.Add(p.ColumnName, p);
                }

            }

        }

    }

    internal class PocoPropertyInfo
    {

        public PropertyInfo Type { get; set; }
        public string ColumnName { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsNullableType { get; set; }
        public bool IsVirtual { get; set; }
        public bool IsNumericType { get; set; }
        public PocoPropertyReferenceType ReferenceType { get; set; }
        public string PocoReferenceKeyName { get; set; }
        public PocoInfo Info { get; set; }

        public enum PocoPropertyReferenceType
        {
            ReferenceContainsKey,
            PocoContainsReferenceKey
        }

        internal void SetValue(object poco, object value)
        {
            if (value != DBNull.Value)
                Type.SetValue(poco, value, null);
        }

        internal object GetValue(object poco)
        {
            return Type.GetValue(poco, null);
        }

    }

    #endregion

    #region Mapper

    public interface IPocoMappingExpression<T>
    {

        IMappingExpression<T> ToTable(string name, bool identityIncrement);

    }

    public interface IMappingExpression<T>
    {

        IMappingExpression<T> ForMember(Expression<Func<T, object>> property, string columnName);
        IMappingExpression<T> Exclude(Expression<Func<T, object>> property);
        IMappingExpression<T> Key(Expression<Func<T, object>> property);
        IMappingExpression<T> ForeignKey<T2>(Expression<Func<T, object>> property);
        IMappingExpression<T> SortBy(Expression<Func<T, object>> property);
        IMappingExpression<T> SortByDescending(Expression<Func<T, object>> property);

    }

    public class MappingExpression<T> : IMappingExpression<T>
    {

        public IMappingExpression<T> ForMember(Expression<Func<T, object>> property, string columnName)
        {

            var name = Helper.GetPropertyNameFromExpression<T>(property);

            var info = Mapper.GetTableInfo(typeof(T));

            info.AddColumnMapping(name, columnName);

            return this;

        }

        public IMappingExpression<T> Exclude(Expression<Func<T, object>> property)
        {

            var name = Helper.GetPropertyNameFromExpression<T>(property);

            var info = Mapper.GetTableInfo(typeof(T));

            info.Exclude(name);

            return this;

        }

        public IMappingExpression<T> Key(Expression<Func<T, object>> property)
        {

            var name = Helper.GetPropertyNameFromExpression<T>(property);

            var info = Mapper.GetTableInfo(typeof(T));

            info.AddPrimaryKey(name);

            return this;

        }

        public IMappingExpression<T> ForeignKey<T2>(Expression<Func<T, object>> property)
        {

            var name = Helper.GetPropertyNameFromExpression<T>(property);

            var info = Mapper.GetTableInfo(typeof(T));

            var info2 = Mapper.GetTableInfo(typeof(T2));

            info.AddForeignKey(name, info2);

            return this;

        }

        public IMappingExpression<T> SortBy(Expression<Func<T, object>> property)
        {

            var name = Helper.GetPropertyNameFromExpression<T>(property);

            var info = Mapper.GetTableInfo(typeof(T));

            info.AddSorting(name, TableInfo.SortDirection.ASC);

            return this;

        }

        public IMappingExpression<T> SortByDescending(Expression<Func<T, object>> property)
        {

            var name = Helper.GetPropertyNameFromExpression<T>(property);

            var info = Mapper.GetTableInfo(typeof(T));

            info.AddSorting(name, TableInfo.SortDirection.DESC);

            return this;

        }
    }

    public class PocoMappingExpression<T> : IPocoMappingExpression<T>
    {

        public IMappingExpression<T> ToTable(string name, bool identityIncrement)
        {

            if (Mapper.Tables.ContainsKey(typeof(T).FullName))
                throw new Exception(string.Format("Type '{0}' already in dictionary.", typeof(T).FullName));

            var info = new TableInfo(name, identityIncrement);

            Mapper.Tables.Add(typeof(T).FullName, info);

            return new MappingExpression<T>();

        }

    }

    public static class Mapper
    {

        internal static Dictionary<string, TableInfo> Tables = new Dictionary<string, TableInfo>();

        internal static TableInfo GetTableInfo(Type t)
        {

            if (Tables.ContainsKey(t.FullName))
                return Tables[t.FullName];
            return new TableInfo(t.Name, false);

        }

        internal static TableInfo GetTableInfo(Type t, string name)
        {
            if (Tables.ContainsKey(t.FullName))
                return Tables[t.FullName];
            return new TableInfo(name, false);
        }

        internal static string GetColumnMapping(Type t, string name)
        {

            TableInfo info = GetTableInfo(t);

            if (info.ColumnMappings != null && info.ColumnMappings.ContainsKey(name))
                return info.ColumnMappings[name];

            return null;

        }

        public static IPocoMappingExpression<T> Map<T>()
        {

            return new PocoMappingExpression<T>();

        }

    }

    #endregion

    #region Extensions (Db.QueryResult<T>)

    public static class QueryResultExtensions
    {

        public static Db.QueryResult<T> Where<T>(this Db.QueryResult<T> source, Expression<Func<T, bool>> predicate)
        {
            return (Db.QueryResult<T>)((IQueryable<T>)source).Where(predicate);
        }

        public static bool Any<T>(this Db.QueryResult<T> source)
        {
            return source.Count > 0;
        }

        public static Db.QueryResult<T> Take<T>(this Db.QueryResult<T> source, int count)
        {
            return (Db.QueryResult<T>)((IQueryable<T>)source).Take(count);
        }

        public static Db.QueryResult<T> Skip<T>(this Db.QueryResult<T> source, int count)
        {
            return (Db.QueryResult<T>)((IQueryable<T>)source).Skip(count);
        }

        public static T FirstOrDefault<T>(this Db.QueryResult<T> source, Expression<Func<T, bool>> predicate)
        {
            return (T)((IQueryable<T>)source).FirstOrDefault(predicate);
        }

    }

    #endregion

}
