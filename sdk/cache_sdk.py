import socket
import struct
from typing import Optional, Union, Any


class CacheSDK:
    
    def __init__(self, host: str = "0.0.0.0", port: int = 7171, timeout: float = 0.1):
        """Initialize the cache client.
        
        Args:
            host: Host of the cache server
            port: Port of the cache server
            timeout: Timeout for requests in seconds
        """
        self.host = host
        self.port = port
        self.timeout = timeout
    
    def _serialize_get(self, key: str) -> bytes:
        """Serialize a GET command."""
        key_bytes = key.encode()
        return struct.pack(f"!BH{len(key_bytes)}s", 1, len(key_bytes), key_bytes)
    
    def _serialize_set(self, key: str, value: Any) -> bytes:
        """Serialize a SET command."""
        key_bytes = key.encode()
        value_bytes = str(value).encode()
        return struct.pack(f"!BH{len(key_bytes)}sH{len(value_bytes)}s", 
                          2, len(key_bytes), key_bytes, len(value_bytes), value_bytes)
    
    def _deserialize_response(self, response: bytes) -> str:
        """Deserialize server response."""
        message_len = struct.unpack("!H", response[:2])[0]
        return response[2:2+message_len].decode()
    
    def _send_command(self, serialized_command: bytes) -> str:
        """Send command to server and get response."""
        channel = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            channel.connect((self.host, self.port))
            channel.send(serialized_command)
            
            channel.settimeout(self.timeout)
            try:
                response = channel.recv(512)
            except socket.timeout:
                response = b"\x00\x00Error: Timeout"
                
            return self._deserialize_response(response)
        finally:
            channel.close()
    
    def set(self, key: str, value: Any) -> bool:
        """Set a key-value pair in the cache.
        
        Args:
            key: The key to set
            value: The value to set
            
        Returns:
            True if key was set successfully, False otherwise
        """
        command = self._serialize_set(key, value)
        response = self._send_command(command)
        
        return response == "Stored"
    
    def get(self, key: str) -> Optional[str]:
        """Get a value from the cache.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value if found, None otherwise
        """
        command = self._serialize_get(key)
        response = self._send_command(command)
        
        if response.startswith("Error") or response == "Key not found":
            return None
        return response


if __name__ == "__main__":
    # Example usage
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