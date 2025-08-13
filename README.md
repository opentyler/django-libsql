# Django + LibSQL / Turso

Django integration for modern [libsql](https://github.com/tursodatabase/libsql-python) / [Turso](https://turso.tech) databases.

This fork has been updated to work with the modern `libsql` Python package (0.1.0+), replacing the deprecated `libsql_client` package.

## Features

- ✅ Full Django ORM compatibility
- ✅ Support for local SQLite databases
- ✅ Support for remote Turso databases
- ✅ Embedded replicas with sync
- ✅ Encrypted database support
- ✅ Full transaction support
- ✅ Type conversions (date, time, datetime, decimal, etc.)
- ✅ Foreign key support
- ✅ JSON field support
- ✅ All Django database features

## Installation

```bash
pip install git+https://github.com/opentyler/django-libsql.git
```

Or add to your `requirements.txt`:
```
git+https://github.com/opentyler/django-libsql.git@main
```

## Configuration

### Local SQLite Database

```python
DATABASES = {
    'default': {
        'ENGINE': 'django_libsql.db.backends.sqlite3',
        'NAME': 'local.db',
    }
}
```

### In-Memory Database

```python
DATABASES = {
    'default': {
        'ENGINE': 'django_libsql.db.backends.sqlite3',
        'NAME': ':memory:',
    }
}
```

### Remote Turso Database

Using the connection string format:
```python
DATABASES = {
    'default': {
        'ENGINE': 'django_libsql.db.backends.sqlite3',
        'NAME': 'libsql://your-database.turso.io?authToken=your-auth-token',
    }
}
```

Or using OPTIONS:
```python
DATABASES = {
    'default': {
        'ENGINE': 'django_libsql.db.backends.sqlite3',
        'NAME': 'libsql://your-database.turso.io',
        'OPTIONS': {
            'auth_token': 'your-auth-token',
            'timeout': 30,  # Connection timeout in seconds
        }
    }
}
```

### Embedded Replica (Local + Sync)

```python
DATABASES = {
    'default': {
        'ENGINE': 'django_libsql.db.backends.sqlite3',
        'NAME': 'local_replica.db',
        'OPTIONS': {
            'sync_url': 'libsql://your-database.turso.io',
            'auth_token': 'your-auth-token',
        }
    }
}
```

### Encrypted Database

```python
DATABASES = {
    'default': {
        'ENGINE': 'django_libsql.db.backends.sqlite3',
        'NAME': 'encrypted.db',
        'OPTIONS': {
            'encryption_key': 'your-secret-encryption-key',
        }
    }
}
```

## Multi-Tenant Usage with Dynamic Credentials

For applications that need to dynamically set database credentials (e.g., per-user databases), you can inject credentials at runtime:

```python
# In your middleware or view
from django.db import connections

def set_user_database(user):
    connection = connections['user_db']
    # Special underscore-prefixed keys for runtime injection
    connection.settings_dict['_turso_url'] = user.turso_db_url
    connection.settings_dict['_turso_auth_token'] = user.turso_auth_token
```

## Advanced Options

```python
DATABASES = {
    'default': {
        'ENGINE': 'django_libsql.db.backends.sqlite3',
        'NAME': 'database.db',
        'OPTIONS': {
            # Connection options
            'timeout': 30,                    # Connection timeout in seconds
            'isolation_level': 'DEFERRED',    # Transaction isolation level
            
            # Remote database options
            'auth_token': 'token',            # Authentication token for Turso
            'sync_url': 'libsql://...',      # URL for embedded replica sync
            
            # Encryption
            'encryption_key': 'key',          # Encryption key for database
            
            # Local file for embedded replicas
            'local_file': 'local.db',         # Local file when using sync_url
        }
    }
}
```

## Running the Example App

### Running a Local LibSQL Server

To start a local LibSQL server for development or testing:

```bash
./scripts/docker.sh
```

### Running Django App

Clone this repository and run the example Django app:

```bash
git clone https://github.com/opentyler/django-libsql.git
cd django-libsql
./scripts/docker.sh
python manage.py migrate
python manage.py runserver
```

### Running Tests

To run tests and verify the integration:

```bash
./scripts/test.sh
```

This script performs a self-lifecycle test:
1. Starts a local LibSQL server using Docker
2. Runs the tests against this server
3. Destroys the server at the end

## Differences from Standard SQLite Backend

1. **Remote Database Support**: Can connect to Turso cloud databases
2. **Embedded Replicas**: Support for local databases that sync with remote
3. **Encryption**: Built-in support for encrypted databases
4. **Modern libsql**: Uses the Rust-based libsql library for better performance

## Migration from libsql_client

If you're migrating from the old `libsql_client`-based version:

1. Update your requirements to use this fork
2. No changes needed to your Django settings
3. The backend is fully compatible with existing code

## Changes in This Fork

### Version 0.2.0
- Complete rewrite to support modern `libsql` package (0.1.0+)
- Removed dependency on deprecated `libsql_client`
- Added comprehensive connection wrapper for sqlite3 compatibility
- Improved transaction handling
- Better support for Turso features (sync, encryption)
- Type conversion improvements
- Full Django ORM compatibility maintained

### Key Implementation Details

The implementation provides several compatibility layers:

1. **LibSQLDatabase**: A wrapper class that mimics Python's sqlite3 module interface
2. **LibSQLConnection**: Handles all connection types (local, remote, replica)
3. **LibSQLCursor**: Manages transactions and type conversions
4. **SQLiteCursorWrapper**: Converts Django parameter styles to SQLite

## Known Issues

Most limitations from the original version have been resolved. The modern libsql package provides better compatibility with Django's expectations.

### Resolved Issues
- ✅ Custom Django functions now work properly
- ✅ Date/time operations using `F()` objects are supported
- ✅ The `dates()` queryset method works correctly
- ✅ Full transaction support with proper isolation levels

## Compatibility

- Python 3.7+
- Django 2.1+
- libsql 0.1.0+

## Self-Hosting

If you want to host your own LibSQL server, refer to the provided Docker script (`./scripts/docker.sh`). This script includes a working server setup along with key generation.

## License

This project is distributed under the MIT license.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

If you encounter any issues or have questions, please open an issue on the GitHub repository.

## Credits

- Original django-libsql by Aaron Kazah
- Fork maintained by OpenTyler
- Built on [libsql-python](https://github.com/tursodatabase/libsql-python) by Turso