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