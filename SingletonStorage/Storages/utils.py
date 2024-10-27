class SimpleRSAChunkEncryptor:
    def __init__(self, public_key, private_key=None):
        # public_key and private_key should be tuples of (e, n) and (d, n) respectively
        self.public_key = public_key
        self.private_key = private_key
        # Calculate chunk size just below the modulus size, with a check
        self.chunk_size = (public_key[1].bit_length() // 8) - 1  # Just below n's byte size
        if self.chunk_size <= 0:
            raise ValueError("The modulus 'n' is too small. Please use a larger key size.")

    def encrypt_chunk(self, chunk):
        """Encrypt a single chunk using RSA public key."""
        e, n = self.public_key
        # Convert chunk to an integer for encryption, then apply modular exponentiation
        chunk_int = int.from_bytes(chunk, byteorder='big')
        encrypted_chunk_int = pow(chunk_int, e, n)
        # Convert back to bytes and pad to fixed size
        return encrypted_chunk_int.to_bytes((n.bit_length() + 7) // 8, byteorder='big')

    def decrypt_chunk(self, encrypted_chunk):
        """Decrypt a single chunk using RSA private key."""
        if not self.private_key:
            raise ValueError("Private key is required for decryption.")
        d, n = self.private_key
        # Convert encrypted chunk to integer, then apply modular exponentiation
        encrypted_chunk_int = int.from_bytes(encrypted_chunk, byteorder='big')
        decrypted_chunk_int = pow(encrypted_chunk_int, d, n)
        # Convert back to bytes and strip padding
        return decrypted_chunk_int.to_bytes(self.chunk_size, byteorder='big').rstrip(b'\x00')

    def encrypt_string(self, plaintext):
        """Encrypt a string by splitting it into chunks."""
        encrypted_chunks = []
        # Convert plaintext string to bytes
        plaintext_bytes = plaintext.encode('utf-8')
        # Process in chunks
        for i in range(0, len(plaintext_bytes), self.chunk_size):
            chunk = plaintext_bytes[i:i + self.chunk_size]
            encrypted_chunk = self.encrypt_chunk(chunk)
            encrypted_chunks.append(encrypted_chunk)
        # Join encrypted chunks with a separator
        return b''.join(encrypted_chunks)

    def decrypt_string(self, encrypted_data):
        """Decrypt a string by decrypting each chunk."""
        if not self.private_key:
            raise ValueError("Private key required for decryption.")
        
        decrypted_chunks = []
        encrypted_chunk_size = (self.public_key[1].bit_length() + 7) // 8  # Full size of n in bytes
        # Process each encrypted chunk
        for i in range(0, len(encrypted_data), encrypted_chunk_size):
            encrypted_chunk = encrypted_data[i:i + encrypted_chunk_size]
            decrypted_chunk = self.decrypt_chunk(encrypted_chunk)
            decrypted_chunks.append(decrypted_chunk)
        # Join decrypted chunks and convert back to string
        return b''.join(decrypted_chunks).decode('utf-8')

# Example Usage
# Replace (e, n) and (d, n) with actual RSA public and private key components
# public_key = (65537, 3233)  # Replace with actual (e, n)
# private_key = (2753, 3233)  # Replace with actual (d, n)

# encryptor = SimpleRSAChunkEncryptor(public_key, private_key)
# plaintext = "This is a large string that we want to encrypt in chunks using RSA."
# print("Original text:", plaintext)

# # Encrypt and decrypt the string
# encrypted_data = encryptor.encrypt_string(plaintext)
# print("Encrypted data:", encrypted_data)

# decrypted_text = encryptor.decrypt_string(encrypted_data)
# print("Decrypted text:", decrypted_text)
