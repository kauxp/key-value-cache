import asyncio
import struct
import mmh3
import time
import signal
import functools
from typing import Dict, Tuple, Optional, Any
from concurrent.futures import ThreadPoolExecutor

MAX_CACHE_SIZE = 1_000_000
MAX_KEY_SIZE = 256
MAX_VALUE_SIZE = 256
BIND_IP = "0.0.0.0"
BIND_PORT = 7171
CACHE_SHARDS = 256  # Increased for less contention
READ_WORKERS = 4    # Thread pool for CPU-bound operations

# Cache data structure - main data
cache_shards: list = []
# Writer locks (only used for writes)
write_locks: list = []
# Response cache for hot keys
response_cache: Dict[str, bytes] = {}

# Thread pool for CPU-bound operations
thread_pool = ThreadPoolExecutor(max_workers=READ_WORKERS)

# Initialize shards with more granularity
for i in range(CACHE_SHARDS):
    cache_shards.append({})
    write_locks.append(asyncio.Lock())

def get_shard_index(key: str) -> int:
    """
    Get the shard index for a key using a fast hash
    
    Args:
        key: The key to hash
        
    Returns:
        The shard index
    """
    return mmh3.hash(key) % CACHE_SHARDS

# Synchronous version of get for direct dict access (no locking)
def _get_from_shard(shard_idx: int, key: str) -> Optional[str]:
    """Direct synchronous access to cache shard"""
    shard = cache_shards[shard_idx]
    return shard.get(key)

# Get item without locking for reads (optimistic)
async def get_item_fast(key: str) -> str:
    """
    Fast track get without locking for reads
    
    Args:
        key: The key to retrieve
        
    Returns:
        The value associated with the key, or "Key not found" if not present
    """
    # Check if we have a cached response
    if key in response_cache:
        return key, True, None  # Use cached response
        
    shard_idx = get_shard_index(key)
    
    # Fast CPU-bound dict lookup in a separate thread
    loop = asyncio.get_running_loop()
    value = await loop.run_in_executor(
        thread_pool, 
        functools.partial(_get_from_shard, shard_idx, key)
    )
    
    if value is not None:
        # Cache the serialized response for hot keys if it's small enough
        if len(value) < 1024:  # Only cache small values
            response_cache[key] = serialize_response(value)
        return value, False, serialize_response(value)
    return "Key not found", False, KEY_NOT_FOUND_RESPONSE

async def put_item(key: str, value: str) -> bool:
    """
    Store an item in the cache using asyncio locks with sharding
    
    Args:
        key: The key to store the value under
        value: The value to store
        
    Returns:
        True if the item was stored successfully, False otherwise
    """
    if len(key) > MAX_KEY_SIZE or len(value) > MAX_VALUE_SIZE:
        return False
    
    shard_idx = get_shard_index(key)
    async with write_locks[shard_idx]:
        cache_shards[shard_idx][key] = value
        # Invalidate response cache
        if key in response_cache:
            del response_cache[key]
        return True

async def delete_item(key: str) -> bool:
    """
    Delete an item from the cache with sharding
    
    Args:
        key: The key to delete
        
    Returns:
        True if the item was deleted, False if not found
    """
    shard_idx = get_shard_index(key)
    async with write_locks[shard_idx]:
        if key in cache_shards[shard_idx]:
            del cache_shards[shard_idx][key]
            # Invalidate response cache
            if key in response_cache:
                del response_cache[key]
            return True
        return False

def serialize_response(message: str) -> bytes:
    """
    Serialize response using fast binary encoding
    
    Args:
        message: The message to serialize
        
    Returns:
        The serialized message
    """
    message_bytes = message.encode()
    return struct.pack(f"!H{len(message_bytes)}s", len(message_bytes), message_bytes)

async def recvall_async(reader, n: int) -> Optional[bytes]:
    """
    Async version of recvall with optimizations
    
    Args:
        reader: The async reader
        n: Number of bytes to receive
        
    Returns:
        The received data or None on error
    """
    try:
        return await reader.readexactly(n)
    except (asyncio.IncompleteReadError, ConnectionError):
        return None

async def deserialize_request_async(reader) -> Optional[Tuple]:
    """
    Optimized request deserialization
    
    Args:
        reader: The async reader
        
    Returns:
        A tuple containing the command and its parameters or None on error
    """
    try:
        # Fast path - read header in a single operation
        header = await recvall_async(reader, 3)
        if not header or len(header) < 3:
            return None
            
        command, key_len = struct.unpack("!BH", header)
        
        # Read key data
        key_data = await recvall_async(reader, key_len)
        if not key_data or len(key_data) < key_len:
            return None
        
        key = key_data.decode()
        
        # Fast path for get operations (most common)
        if command == 1:  # Get command
            return ("get", key)
        elif command == 2:  # Set command
            value_len_data = await recvall_async(reader, 2)
            if not value_len_data:
                return None
                
            value_len = struct.unpack("!H", value_len_data)[0]
            
            value_data = await recvall_async(reader, value_len)
            if not value_data:
                return None
                
            value = value_data.decode()
            return ("set", key, value)
        elif command == 3:  # Delete command
            return ("delete", key)
        else:
            return None
    except Exception:
        return None

async def handle_client_async(reader, writer):
    """
    Optimized async handler for client connections with improved connection handling
    
    Args:
        reader: The async reader
        writer: The async writer
    """
    try:
        # Fast path for requests
        command = await deserialize_request_async(reader)
        
        if not command:
            response_data = INVALID_REQ_RESPONSE
        else:
            try:
                if command[0] == "get":
                    # Super optimized get path
                    value, cached, serialized_response = await get_item_fast(command[1])
                    if cached:
                        # Use pre-cached response
                        response_data = response_cache[command[1]]
                    elif serialized_response:
                        # Use pre-serialized response
                        response_data = serialized_response
                    else:
                        # Fallback to standard response
                        response_data = KEY_NOT_FOUND_RESPONSE
                elif command[0] == "set":
                    success = await put_item(command[1], command[2])
                    response_data = STORED_RESPONSE if success else FAILED_RESPONSE
                elif command[0] == "delete":
                    success = await delete_item(command[1])
                    response_data = DELETED_RESPONSE if success else KEY_NOT_FOUND_RESPONSE
                else:
                    response_data = UNKNOWN_CMD_RESPONSE
            except Exception:
                response_data = UNKNOWN_CMD_RESPONSE
        
        # Write response safely
        try:
            writer.write(response_data)
            # Always drain to ensure data is sent before closing
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError) as e:
            # Connection was reset or broken pipe, just log and continue
            pass
        except Exception:
            # Other errors, just continue to close the connection properly
            pass
            
    except Exception:
        # Exception during request handling
        pass
    finally:
        # Proper connection closing to avoid "connection reset by peer"
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            # Ignore errors during connection closing
            pass

async def main():
    # Pre-compute common responses for maximum performance
    global INVALID_REQ_RESPONSE, STORED_RESPONSE, FAILED_RESPONSE
    global KEY_NOT_FOUND_RESPONSE, DELETED_RESPONSE, UNKNOWN_CMD_RESPONSE
    
    INVALID_REQ_RESPONSE = serialize_response("Invalid request")
    STORED_RESPONSE = serialize_response("Stored")
    FAILED_RESPONSE = serialize_response("Failed")
    KEY_NOT_FOUND_RESPONSE = serialize_response("Key not found")
    DELETED_RESPONSE = serialize_response("Deleted")
    UNKNOWN_CMD_RESPONSE = serialize_response("Unknown command")
    
    # Create server with highly optimized parameters for low latency
    server = await asyncio.start_server(
        handle_client_async, 
        BIND_IP, 
        BIND_PORT,
        backlog=8192,  # Increased backlog
        reuse_address=True,
        start_serving=True,
        limit=65536,   # Larger buffer size
    )
    
    print(f"[+] Optimized read server listening on {BIND_IP}:{BIND_PORT}")
    print(f"[+] Using {CACHE_SHARDS} cache shards with optimized read path")
    
    # Setup graceful shutdown
    loop = asyncio.get_running_loop()
    
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(
            getattr(signal, signame),
            lambda: asyncio.create_task(shutdown(server))
        )
    
    async with server:
        await server.serve_forever()

async def shutdown(server):
    """
    Graceful shutdown of the server
    
    Args:
        server: The asyncio server
    """
    print("[-] Server shutting down...")
    server.close()
    await server.wait_closed()
    
    # Shutdown thread pool
    thread_pool.shutdown(wait=False)
    
    # Cancel all running tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    
    for task in tasks:
        task.cancel()
    
    await asyncio.gather(*tasks, return_exceptions=True)
    asyncio.get_event_loop().stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[-] Server interrupted")