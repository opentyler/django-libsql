"""
libSQL backend for Django using the new python-libsql package.
"""
import datetime
import decimal
import sys
import warnings
from collections.abc import Mapping
from itertools import chain, tee
from typing import Optional, Dict, Any
from urllib.parse import urlparse, parse_qs

# Import the libsql package - no namespace conflict since we renamed local directory
import libsql as libsql_client

from django.core.exceptions import ImproperlyConfigured
from django.db import IntegrityError
from django.db.backends.base.base import BaseDatabaseWrapper
from django.utils.asyncio import async_unsafe
from django.utils.dateparse import parse_date, parse_datetime, parse_time
from django.utils.regex_helper import _lazy_re_compile


def decoder(conv_func):
    """
    Convert bytestrings from Python's sqlite3 interface to a regular string.
    """
    return lambda s: conv_func(s.decode()) if isinstance(s, bytes) else conv_func(s)


def adapt_date(val):
    """Adapt date to ISO format string."""
    return val.isoformat()


def adapt_datetime(val):
    """Adapt datetime to ISO format string."""
    return val.isoformat(" ")


def adapt_decimal_to_str(val):
    """Adapt Decimal to string for storage."""
    return str(val)


def convert_bool(val):
    """Convert database value to Python bool."""
    if isinstance(val, bytes):
        return val == b'1'
    return bool(val) if val is not None else None


def convert_date(val):
    """Convert database value to Python date."""
    if val is None:
        return None
    if isinstance(val, bytes):
        val = val.decode()
    return parse_date(val)


def convert_datetime(val):
    """Convert database value to Python datetime."""
    if val is None:
        return None
    if isinstance(val, bytes):
        val = val.decode()
    return parse_datetime(val)


def convert_time(val):
    """Convert database value to Python time."""
    if val is None:
        return None
    if isinstance(val, bytes):
        val = val.decode()
    return parse_time(val)


class LibSQLDatabase:
    """
    Compatibility wrapper to provide sqlite3-like interface for libsql.
    
    This class mimics the sqlite3 module interface expected by Django,
    while using the modern libsql package underneath.
    """
    
    # SQLite/LibSQL constants (for compatibility)
    PARSE_DECLTYPES = 1
    PARSE_COLNAMES = 2
    
    # Version information (mimicking sqlite3)
    sqlite_version = "3.35.0"
    sqlite_version_info = (3, 35, 0)
    version = "2.6.0"
    version_info = (2, 6, 0)
    
    # Standard exceptions (for compatibility)
    Error = libsql_client.Error if hasattr(libsql_client, 'Error') else Exception
    InterfaceError = Error
    DatabaseError = Error
    DataError = DatabaseError
    OperationalError = DatabaseError
    IntegrityError = DatabaseError
    InternalError = DatabaseError
    ProgrammingError = DatabaseError
    NotSupportedError = DatabaseError
    
    @staticmethod
    def connect(**kwargs):
        """Create a libsql connection with Django-compatible parameters."""
        return LibSQLConnection(**kwargs)
    
    @staticmethod
    def register_converter(typename, converter):
        """Register type converter (stored for manual application)."""
        # libsql doesn't support global converters, we'll handle this in cursor
        if not hasattr(LibSQLDatabase, '_converters'):
            LibSQLDatabase._converters = {}
        LibSQLDatabase._converters[typename] = converter
    
    @staticmethod
    def register_adapter(type_obj, adapter):
        """Register type adapter (stored for manual application)."""
        # libsql doesn't support global adapters, we'll handle this in cursor
        if not hasattr(LibSQLDatabase, '_adapters'):
            LibSQLDatabase._adapters = {}
        LibSQLDatabase._adapters[type_obj] = adapter


class LibSQLConnection:
    """
    Connection wrapper that provides sqlite3-compatible interface for libsql.
    """
    
    def __init__(self, **kwargs):
        """
        Initialize connection with libsql.
        
        Supports:
        - database: Path to database file, ":memory:", or remote URL
        - auth_token: Authentication token for remote databases
        - sync_url: URL for syncing embedded replicas
        - encryption_key: Key for encrypted databases
        - timeout: Connection timeout in seconds
        - isolation_level: Transaction isolation level
        - check_same_thread: Ignored (for compatibility)
        - uri: Ignored (for compatibility)
        - detect_types: Ignored (we handle types manually)
        """
        self.database = kwargs.get('database', ':memory:')
        self.auth_token = kwargs.get('auth_token', '')
        self.sync_url = kwargs.get('sync_url', None)
        self.encryption_key = kwargs.get('encryption_key', None)
        self.timeout = kwargs.get('timeout', 5.0)
        self._isolation_level = kwargs.get('isolation_level', '')
        
        # Parse Turso URL if provided
        if self.database.startswith('libsql://'):
            # Handle Turso URL format
            parsed = urlparse(self.database)
            if parsed.netloc:
                # Convert libsql:// to https:// for the actual connection
                self.database = f"https://{parsed.netloc}{parsed.path}"
            
            # Extract auth token from URL if present
            if parsed.query:
                params = parse_qs(parsed.query)
                if 'authToken' in params:
                    self.auth_token = params['authToken'][0]
        
        # Create the actual libsql connection
        conn_params = {}
        
        # Determine connection type
        if self.database == ':memory:' or self.database.startswith('file:'):
            # In-memory or local file database
            conn_params['database'] = self.database
        elif self.database.startswith(('http://', 'https://', 'ws://', 'wss://')):
            # Remote database
            conn_params['database'] = self.database
            if self.auth_token:
                conn_params['auth_token'] = self.auth_token
        else:
            # Regular file path
            conn_params['database'] = self.database
        
        # Add sync URL if provided (for embedded replicas)
        if self.sync_url:
            conn_params['sync_url'] = self.sync_url
            if self.auth_token:
                conn_params['auth_token'] = self.auth_token
            
        # Add encryption key if provided
        if self.encryption_key:
            conn_params['encryption_key'] = self.encryption_key
            
        # Special handling for injected Turso credentials from turso app
        if '_turso_url' in kwargs:
            # Use embedded replica mode with local file
            conn_params['database'] = kwargs.get('local_file', 'local.db')
            conn_params['sync_url'] = kwargs['_turso_url']
            if '_turso_auth_token' in kwargs:
                conn_params['auth_token'] = kwargs['_turso_auth_token']
        
        try:
            self._connection = libsql_client.connect(**conn_params)
        except Exception as e:
            raise ImproperlyConfigured(f"Could not connect to libsql database: {e}")
        
        # Track transaction state
        self._in_transaction = False
        
    @property
    def isolation_level(self):
        """Get current isolation level."""
        return self._isolation_level
    
    @isolation_level.setter
    def isolation_level(self, value):
        """
        Set isolation level.
        
        None = autocommit mode
        "" or other = manual transaction mode
        """
        self._isolation_level = value
        if value is None and self._connection.in_transaction:
            # Commit any pending transaction when switching to autocommit
            try:
                self.commit()
            except:
                self.rollback()
    
    def cursor(self, factory=None):
        """Create a cursor object."""
        base_cursor = self._connection.cursor()
        if factory:
            return factory(self, base_cursor)
        return LibSQLCursor(self, base_cursor)
    
    def commit(self):
        """Commit the current transaction."""
        if self._isolation_level is not None:  # Not in autocommit mode
            self._connection.commit()
            # Update our tracking to match libsql's state
            self._in_transaction = self._connection.in_transaction
    
    def rollback(self):
        """Rollback the current transaction."""
        if self._isolation_level is not None:  # Not in autocommit mode
            self._connection.rollback()
            # Update our tracking to match libsql's state
            self._in_transaction = self._connection.in_transaction
    
    def close(self):
        """Close the connection."""
        self._connection.close()
    
    def execute(self, sql, params=None):
        """Execute SQL directly on connection."""
        cursor = self.cursor()
        return cursor.execute(sql, params)
    
    def executemany(self, sql, params_list):
        """Execute SQL with multiple parameter sets."""
        cursor = self.cursor()
        return cursor.executemany(sql, params_list)
    
    def executescript(self, sql_script):
        """Execute multiple SQL statements."""
        return self._connection.executescript(sql_script)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if exc_type:
            self.rollback()
        else:
            self.commit()
        return False


class LibSQLCursor:
    """
    Cursor wrapper that provides sqlite3-compatible interface for libsql cursors.
    """
    
    def __init__(self, connection, base_cursor):
        """Initialize cursor wrapper."""
        self.connection = connection
        self._cursor = base_cursor
        self.lastrowid = None
        self.rowcount = -1
        self.description = None
        self.arraysize = 100
    
    def execute(self, sql, params=None):
        """Execute a SQL statement."""
        # Start transaction if needed - trust libsql's transaction tracking
        if (self.connection._isolation_level is not None and 
            not self.connection._connection.in_transaction and
            self._is_dml(sql)):
            # Start implicit transaction for DML in non-autocommit mode
            self._cursor.execute("BEGIN")
            # Update our tracking to match libsql's state
            self.connection._in_transaction = self.connection._connection.in_transaction
        
        # Apply parameter adapters
        if params:
            params = self._adapt_params(params)
        
        # Execute the query
        result = self._cursor.execute(sql, params) if params else self._cursor.execute(sql)
        
        # Update cursor state
        self.lastrowid = getattr(self._cursor, 'lastrowid', None)
        self.rowcount = getattr(self._cursor, 'rowcount', -1)
        self.description = getattr(self._cursor, 'description', None)
        
        return self
    
    def executemany(self, sql, params_list):
        """Execute SQL with multiple parameter sets."""
        for params in params_list:
            self.execute(sql, params)
        return self
    
    def fetchone(self):
        """Fetch one row from results."""
        row = self._cursor.fetchone()
        if row:
            return self._convert_row(row)
        return row
    
    def fetchmany(self, size=None):
        """Fetch multiple rows from results."""
        if size is None:
            size = self.arraysize
        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows
    
    def fetchall(self):
        """Fetch all remaining rows from results."""
        rows = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows
    
    def close(self):
        """Close the cursor."""
        # libsql cursors don't have explicit close
        pass
    
    def __iter__(self):
        """Iterate over results."""
        return self
    
    def __next__(self):
        """Get next row from results."""
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row
    
    def _is_dml(self, sql):
        """Check if SQL is a DML statement."""
        sql_upper = sql.strip().upper()
        return any(sql_upper.startswith(cmd) for cmd in ('INSERT', 'UPDATE', 'DELETE', 'REPLACE'))
    
    def _adapt_params(self, params):
        """Apply registered adapters to parameters."""
        if not hasattr(LibSQLDatabase, '_adapters'):
            return params
        
        adapters = LibSQLDatabase._adapters
        
        if isinstance(params, (list, tuple)):
            adapted = []
            for param in params:
                param_type = type(param)
                if param_type in adapters:
                    param = adapters[param_type](param)
                adapted.append(param)
            return tuple(adapted) if isinstance(params, tuple) else adapted
        elif isinstance(params, dict):
            adapted = {}
            for key, param in params.items():
                param_type = type(param)
                if param_type in adapters:
                    param = adapters[param_type](param)
                adapted[key] = param
            return adapted
        else:
            return params
    
    def _convert_row(self, row):
        """Apply registered converters to row data."""
        if not hasattr(LibSQLDatabase, '_converters') or not self.description:
            return row
        
        converters = LibSQLDatabase._converters
        converted = []
        
        for i, (col_desc, value) in enumerate(zip(self.description, row)):
            if value is not None and col_desc:
                col_name = col_desc[0] if isinstance(col_desc, tuple) else str(col_desc)
                # Check for type hints in column name (e.g., "created [timestamp]")
                for typename, converter in converters.items():
                    if f"[{typename}]" in col_name.lower() or typename in col_name.lower():
                        try:
                            value = converter(value)
                        except:
                            pass
                        break
            converted.append(value)
        
        return tuple(converted)


# Create module-like interface (must be before imports that use it)
Database = LibSQLDatabase

# Import Django backend components after Database is defined
from .client import DatabaseClient
from .creation import DatabaseCreation
from .features import DatabaseFeatures
from .introspection import DatabaseIntrospection
from .operations import DatabaseOperations
from .schema import DatabaseSchemaEditor

# Register type converters
Database.register_converter("bool", convert_bool)
Database.register_converter("date", convert_date)
Database.register_converter("datetime", convert_datetime)
Database.register_converter("time", convert_time)
Database.register_converter("timestamp", convert_datetime)

# Register type adapters
Database.register_adapter(decimal.Decimal, adapt_decimal_to_str)
Database.register_adapter(datetime.date, adapt_date)
Database.register_adapter(datetime.datetime, adapt_datetime)


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = "libsql" 
    display_name = "libSQL"
    
    def __init__(self, settings_dict, alias):
        # Store the arguments that Django passes
        self.settings_dict = settings_dict
        self.alias = alias
        self.queries_log = []
        self.force_debug_cursor = False
        
        # Initialize connection to None (will be created lazily)
        self.connection = None
        
        # Try to call parent init, but don't fail if it doesn't work
        try:
            super().__init__(settings_dict, alias)
        except TypeError:
            # If BaseDatabaseWrapper doesn't have proper __init__, 
            # manually initialize the required attributes
            self.features = self.features_class(self)
            self.ops = self.ops_class(self)
            self.client = self.client_class(self)
            self.creation = self.creation_class(self)
            self.introspection = self.introspection_class(self)
            self.validation = None  # Django's validation class if needed
    
    # Data types match SQLite
    data_types = {
        "AutoField": "integer",
        "BigAutoField": "integer",
        "BinaryField": "BLOB",
        "BooleanField": "bool",
        "CharField": "varchar(%(max_length)s)",
        "DateField": "date",
        "DateTimeField": "datetime",
        "DecimalField": "decimal",
        "DurationField": "bigint",
        "FileField": "varchar(%(max_length)s)",
        "FilePathField": "varchar(%(max_length)s)",
        "FloatField": "real",
        "IntegerField": "integer",
        "BigIntegerField": "bigint",
        "IPAddressField": "char(15)",
        "GenericIPAddressField": "char(39)",
        "JSONField": "text",
        "OneToOneField": "integer",
        "PositiveBigIntegerField": "bigint unsigned",
        "PositiveIntegerField": "integer unsigned",
        "PositiveSmallIntegerField": "smallint unsigned",
        "SlugField": "varchar(%(max_length)s)",
        "SmallAutoField": "integer",
        "SmallIntegerField": "smallint",
        "TextField": "text",
        "TimeField": "time",
        "UUIDField": "char(32)",
    }
    
    data_type_check_constraints = {
        "PositiveBigIntegerField": '"%(column)s" >= 0',
        "JSONField": '(JSON_VALID("%(column)s") OR "%(column)s" IS NULL)',
        "PositiveIntegerField": '"%(column)s" >= 0',
        "PositiveSmallIntegerField": '"%(column)s" >= 0',
    }
    
    data_types_suffix = {
        "AutoField": "AUTOINCREMENT",
        "BigAutoField": "AUTOINCREMENT",
        "SmallAutoField": "AUTOINCREMENT",
    }
    
    # Operators match SQLite
    operators = {
        "exact": "= %s",
        "iexact": "LIKE %s ESCAPE '\\'",
        "contains": "LIKE %s ESCAPE '\\'",
        "icontains": "LIKE %s ESCAPE '\\'",
        "regex": "REGEXP %s",
        "iregex": "REGEXP '(?i)' || %s",
        "gt": "> %s",
        "gte": ">= %s",
        "lt": "< %s",
        "lte": "<= %s",
        "startswith": "LIKE %s ESCAPE '\\'",
        "endswith": "LIKE %s ESCAPE '\\'",
        "istartswith": "LIKE %s ESCAPE '\\'",
        "iendswith": "LIKE %s ESCAPE '\\'",
    }
    
    pattern_esc = r"REPLACE(REPLACE(REPLACE({}, '\', '\\'), '%%', '\%%'), '_', '\_')"
    pattern_ops = {
        "contains": r"LIKE '%%' || {} || '%%' ESCAPE '\'",
        "icontains": r"LIKE '%%' || UPPER({}) || '%%' ESCAPE '\'",
        "startswith": r"LIKE {} || '%%' ESCAPE '\'",
        "istartswith": r"LIKE UPPER({}) || '%%' ESCAPE '\'",
        "endswith": r"LIKE '%%' || {} ESCAPE '\'",
        "iendswith": r"LIKE '%%' || UPPER({}) ESCAPE '\'",
    }
    
    Database = Database
    SchemaEditorClass = DatabaseSchemaEditor
    
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations
    
    def get_connection_params(self):
        """
        Get connection parameters for libsql.
        
        Supports various connection modes:
        - Local SQLite files
        - In-memory databases
        - Remote Turso databases
        - Embedded replicas with sync
        - Encrypted databases
        """
        settings_dict = self.settings_dict
        if not settings_dict["NAME"]:
            raise ImproperlyConfigured(
                "settings.DATABASES is improperly configured. "
                "Please supply the NAME value."
            )
        
        db_name = settings_dict["NAME"]
        options = settings_dict.get("OPTIONS", {})
        
        kwargs = {
            "database": db_name,
            "timeout": options.get("timeout", 5.0),
        }
        
        # Check for Turso/remote database configuration
        if db_name.startswith(("libsql://", "http://", "https://", "ws://", "wss://")):
            # Remote database URL provided
            kwargs["database"] = db_name
            
            # Auth token for remote databases
            if "auth_token" in options:
                kwargs["auth_token"] = options["auth_token"]
            elif "authToken" in options:
                kwargs["auth_token"] = options["authToken"]
                
        # Check for embedded replica configuration
        if "sync_url" in options:
            kwargs["sync_url"] = options["sync_url"]
            if db_name.startswith(("libsql://", "http://", "https://", "ws://", "wss://")):
                # If NAME is a remote URL and sync_url is provided,
                # use a local file and sync from the remote
                kwargs["database"] = options.get("local_file", "local.db")
                kwargs["sync_url"] = db_name
        
        # Check for encryption
        if "encryption_key" in options:
            kwargs["encryption_key"] = options["encryption_key"]
        
        # Transaction isolation level
        if "isolation_level" in options:
            kwargs["isolation_level"] = options["isolation_level"]
        else:
            # Default to DEFERRED for compatibility
            kwargs["isolation_level"] = "DEFERRED"
        
        # Check for injected Turso credentials (from turso app)
        if "_turso_url" in settings_dict:
            kwargs["_turso_url"] = settings_dict["_turso_url"]
        if "_turso_auth_token" in settings_dict:
            kwargs["_turso_auth_token"] = settings_dict["_turso_auth_token"]
        
        # Ignored for compatibility but included if present
        if "check_same_thread" in options:
            if options["check_same_thread"]:
                warnings.warn(
                    "The `check_same_thread` option was provided and set to "
                    "True. It will be overridden with False. Use the "
                    "`DatabaseWrapper.allow_thread_sharing` property instead "
                    "for controlling thread shareability.",
                    RuntimeWarning,
                )
        
        # Add detect_types for compatibility
        kwargs["detect_types"] = Database.PARSE_DECLTYPES | Database.PARSE_COLNAMES
        kwargs["check_same_thread"] = False
        kwargs["uri"] = True
        
        return kwargs
    
    def get_database_version(self):
        """Get database version info."""
        return Database.sqlite_version_info
    
    @async_unsafe
    def get_new_connection(self, conn_params):
        """Create a new database connection."""
        conn = Database.connect(**conn_params)
        
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")
        
        # Sync if this is an embedded replica
        if hasattr(conn._connection, 'sync') and conn_params.get('sync_url'):
            try:
                conn._connection.sync()
            except:
                pass  # Sync errors shouldn't prevent connection
        
        return conn
    
    def create_cursor(self, name=None):
        """Create a database cursor."""
        return self.connection.cursor(factory=SQLiteCursorWrapper)
    
    def validate_thread_sharing(self):
        """
        Validate that the connection can be shared between threads.
        This is a no-op for libSQL as it handles thread safety internally.
        """
        pass
    
    @async_unsafe
    def close(self):
        """Close the database connection."""
        self.validate_thread_sharing()
        # If database is in memory, closing the connection destroys the
        # database. To prevent accidental data loss, ignore close requests on
        # an in-memory db.
        if not self.is_in_memory_db():
            # Close the actual connection if it exists
            if self.connection is not None:
                try:
                    self.connection.close()
                except:
                    pass
                self.connection = None
            # Try to call parent's close if it exists
            if hasattr(BaseDatabaseWrapper, 'close'):
                try:
                    BaseDatabaseWrapper.close(self)
                except:
                    pass
    
    def _savepoint_allowed(self):
        """Check if savepoints are allowed."""
        # When 'isolation_level' is not None, sqlite3 commits before each
        # savepoint; it's a bug. When it is None, savepoints don't make sense
        # because autocommit is enabled. The only exception is inside 'atomic'
        # blocks. To work around that bug, on SQLite, 'atomic' starts a
        # transaction explicitly rather than simply disable autocommit.
        return self.in_atomic_block
    
    def _set_autocommit(self, autocommit):
        """Set autocommit mode."""
        if autocommit:
            level = None
        else:
            # sqlite3's internal default is ''. It's different from None.
            # See Modules/_sqlite/connection.c.
            level = ""
        # 'isolation_level' is a misleading API.
        # SQLite always runs at the SERIALIZABLE isolation level.
        with self.wrap_database_errors:
            self.connection.isolation_level = level
    
    def disable_constraint_checking(self):
        """Disable foreign key constraint checking."""
        with self.cursor() as cursor:
            cursor.execute("PRAGMA foreign_keys = OFF")
            # Foreign key constraints cannot be turned off while in a multi-
            # statement transaction. Fetch the current state of the pragma
            # to determine if constraints are effectively disabled.
            enabled = cursor.execute("PRAGMA foreign_keys").fetchone()[0]
        return not bool(enabled)
    
    def enable_constraint_checking(self):
        """Enable foreign key constraint checking."""
        with self.cursor() as cursor:
            cursor.execute("PRAGMA foreign_keys = ON")
    
    def check_constraints(self, table_names=None):
        """
        Check each table name in `table_names` for rows with invalid foreign
        key references. This method is intended to be used in conjunction with
        `disable_constraint_checking()` and `enable_constraint_checking()`, to
        determine if rows with invalid references were entered while constraint
        checks were off.
        """
        with self.cursor() as cursor:
            if table_names is None:
                violations = cursor.execute("PRAGMA foreign_key_check").fetchall()
            else:
                violations = chain.from_iterable(
                    cursor.execute(
                        "PRAGMA foreign_key_check(%s)" % self.ops.quote_name(table_name)
                    ).fetchall()
                    for table_name in table_names
                )
            # See https://www.sqlite.org/pragma.html#pragma_foreign_key_check
            for (
                table_name,
                rowid,
                referenced_table_name,
                foreign_key_index,
            ) in violations:
                foreign_key = cursor.execute(
                    "PRAGMA foreign_key_list(%s)" % self.ops.quote_name(table_name)
                ).fetchall()[foreign_key_index]
                column_name, referenced_column_name = foreign_key[3:5]
                primary_key_column_name = self.introspection.get_primary_key_column(
                    cursor, table_name
                )
                primary_key_value, bad_value = cursor.execute(
                    "SELECT %s, %s FROM %s WHERE rowid = %%s"
                    % (
                        self.ops.quote_name(primary_key_column_name),
                        self.ops.quote_name(column_name),
                        self.ops.quote_name(table_name),
                    ),
                    (rowid,),
                ).fetchone()
                raise IntegrityError(
                    "The row in table '%s' with primary key '%s' has an "
                    "invalid foreign key: %s.%s contains a value '%s' that "
                    "does not have a corresponding value in %s.%s."
                    % (
                        table_name,
                        primary_key_value,
                        table_name,
                        column_name,
                        bad_value,
                        referenced_table_name,
                        referenced_column_name,
                    )
                )
    
    def is_usable(self):
        """Check if the database connection is usable."""
        return True
    
    def _start_transaction_under_autocommit(self):
        """
        Start a transaction explicitly in autocommit mode.
        
        Staying in autocommit mode works around a bug of sqlite3 that breaks
        savepoints when autocommit is disabled.
        """
        # Trust libsql to handle transactions - only start if not already in one
        if not self.connection._connection.in_transaction:
            self.cursor().execute("BEGIN")
    
    def is_in_memory_db(self):
        """Check if this is an in-memory database."""
        return self.creation.is_in_memory_db(self.settings_dict["NAME"])


FORMAT_QMARK_REGEX = _lazy_re_compile(r"(?<!%)%s")


class SQLiteCursorWrapper:
    """
    Django uses the "format" and "pyformat" styles, but Python's sqlite3 module
    supports neither of these styles.
    
    This wrapper performs the following conversions:
    - "format" style to "qmark" style
    - "pyformat" style to "named" style
    
    In both cases, if you want to use a literal "%s", you'll need to use "%%s".
    """
    
    def __init__(self, connection, cursor):
        """Initialize the cursor wrapper."""
        self.connection = connection
        self.cursor = cursor
    
    def execute(self, query, params=None):
        """Execute a SQL query with parameter conversion."""
        if params is None:
            return self.cursor.execute(query)
        # Extract names if params is a mapping, i.e. "pyformat" style is used.
        param_names = list(params) if isinstance(params, Mapping) else None
        query = self.convert_query(query, param_names=param_names)
        return self.cursor.execute(query, params)
    
    def executemany(self, query, param_list):
        """Execute a SQL query with multiple parameter sets."""
        # Extract names if params is a mapping, i.e. "pyformat" style is used.
        # Peek carefully as a generator can be passed instead of a list/tuple.
        peekable, param_list = tee(iter(param_list))
        if (params := next(peekable, None)) and isinstance(params, Mapping):
            param_names = list(params)
        else:
            param_names = None
        query = self.convert_query(query, param_names=param_names)
        return self.cursor.executemany(query, param_list)
    
    def convert_query(self, query, *, param_names=None):
        """Convert query parameter style from Django to SQLite."""
        if param_names is None:
            # Convert from "format" style to "qmark" style.
            return FORMAT_QMARK_REGEX.sub("?", query).replace("%%", "%")
        else:
            # Convert from "pyformat" style to "named" style.
            return query % {name: f":{name}" for name in param_names}
    
    def fetchone(self):
        """Fetch one row from the result set."""
        return self.cursor.fetchone()
    
    def fetchmany(self, size=None):
        """Fetch multiple rows from the result set."""
        return self.cursor.fetchmany(size)
    
    def fetchall(self):
        """Fetch all remaining rows from the result set."""
        return self.cursor.fetchall()
    
    def close(self):
        """Close the cursor."""
        return self.cursor.close()
    
    def __getattr__(self, name):
        """Proxy all other attributes to the underlying cursor."""
        return getattr(self.cursor, name)
    
    def __iter__(self):
        """Make cursor iterable."""
        return iter(self.cursor)
