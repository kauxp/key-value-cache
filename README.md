# Key Value Cache

This repo lightweight and fast key-value cache for Python. It is designed to be simple and easy to use, with a focus on performance.

# How is it made

The server is a TCP server runnning on port 7171 that will listen for the `get` and `set` commands. The server can be started using the following command from the root of the project:

```bash
uv run server
```

The sdk is present in the SDK folder and can be used to interact with the server.

```python
from sdk import CacheSDK

cache = CacheSDK()

# Set a value
success = cache.set("user:1", "John Doe")
print(f"Set success: {success}")

# Get a value
value = cache.get("user:1")
print(f"Got value: {value}")

# Get a non-existent value
value = cache.get("nonexistent")
print(f"Got non-existent value: {value}")
```
