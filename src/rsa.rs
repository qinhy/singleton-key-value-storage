use base64;
use num_bigint::BigUint;
use num_traits::ToPrimitive;
use std::fs::File;
use std::io::{BufRead, BufReader};


pub struct PEMFileReader {
    key_bytes: Vec<u8>,
}

impl PEMFileReader {
    pub fn new(file_path: &str) -> std::io::Result<Self> {
        let key_bytes = Self::read_pem_file(file_path)?;
        Ok(Self { key_bytes })
    }

    fn read_pem_file(file_path: &str) -> std::io::Result<Vec<u8>> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let mut key_data = String::new();
        for line in reader.lines() {
            let line = line?;
            if !line.starts_with("-----BEGIN") && !line.starts_with("-----END") {
                key_data.push_str(&line);
            }
        }
        base64::decode(key_data).map_err(
            |e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    fn parse_asn1_der_element(data: &[u8], index: &mut usize) -> (u8, usize, Vec<u8>) {
        let tag = data[*index];
        *index += 1;

        let length_byte = data[*index];
        *index += 1;
        let length = if length_byte & 0x80 == 0 {
            (length_byte & 0x7F) as usize
        } else {
            let num_length_bytes = (length_byte & 0x7F) as usize;
            let length = BigUint::from_bytes_be(&data[*index..*index + num_length_bytes])
                .to_usize()
                .expect("Failed to convert length");
            *index += num_length_bytes;
            length
        };

        let value = data[*index..*index + length].to_vec();
        *index += length;

        (tag, length, value)
    }

    fn parse_asn1_der_integer(data: &[u8], index: &mut usize) -> BigUint {
        let (tag, _, value) = Self::parse_asn1_der_element(data, index);
        if tag != 0x02 {
            panic!("Expected INTEGER");
        }
        BigUint::from_bytes_be(&value)
    }

    fn parse_asn1_der_sequence(data: &[u8], index: &mut usize) -> Vec<u8> {
        let (tag, _, value) = Self::parse_asn1_der_element(data, index);
        if tag != 0x30 {
            panic!("Expected SEQUENCE");
        }
        value
    }

    pub fn load_public_pkcs8_key(&self) -> (BigUint, BigUint) {
        let mut index = 0;
        let data = Self::parse_asn1_der_sequence(&self.key_bytes, &mut index);

        let mut index = 0;
        let _algorithm_id = Self::parse_asn1_der_sequence(&data, &mut index);

        let (tag, _, value) = Self::parse_asn1_der_element(&data, &mut index);
        if tag != 0x03 {
            panic!("Expected BIT STRING");
        }
        if value[0] != 0x00 {
            panic!("Invalid BIT STRING padding");
        }
        let public_key_bytes = &value[1..];

        let mut index = 0;
        let rsa_key_data = Self::parse_asn1_der_sequence(public_key_bytes, &mut index);

        let mut index = 0;
        let n = Self::parse_asn1_der_integer(&rsa_key_data, &mut index);
        let e = Self::parse_asn1_der_integer(&rsa_key_data, &mut index);

        (e, n)
    }

    pub fn load_private_pkcs8_key(&self) -> (BigUint, BigUint) {
        let mut index = 0;
        let data = Self::parse_asn1_der_sequence(&self.key_bytes, &mut index);

        let mut index = 0;
        let _version = Self::parse_asn1_der_integer(&data, &mut index);

        let _algorithm_id = Self::parse_asn1_der_sequence(&data, &mut index);

        let (tag, _, private_key_bytes) = Self::parse_asn1_der_element(&data, &mut index);
        if tag != 0x04 {
            panic!("Expected OCTET STRING");
        }

        let mut index = 0;
        let rsa_key_data = Self::parse_asn1_der_sequence(&private_key_bytes, &mut index);

        let mut index = 0;
        let _version = Self::parse_asn1_der_integer(&rsa_key_data, &mut index);

        let n = Self::parse_asn1_der_integer(&rsa_key_data, &mut index);
        Self::parse_asn1_der_integer(&rsa_key_data, &mut index);
        let d = Self::parse_asn1_der_integer(&rsa_key_data, &mut index);

        (d, n)
    }
}

pub struct SimpleRSAChunkEncryptor {
    public_key: Option<(BigUint, BigUint)>,
    private_key: Option<(BigUint, BigUint)>,
    chunk_size: usize,
}

impl SimpleRSAChunkEncryptor {
    pub fn new(public_key: Option<(BigUint, BigUint)>, private_key: Option<(BigUint, BigUint)>) -> Self {
        let chunk_size = if let Some((_, n)) = &public_key {
            (n.bits() as usize / 8) - 1
        } else {
            0
        };
        if chunk_size <= 0 {
            panic!("The modulus 'n' is too small. Please use a larger key size.");
        }

        Self {
            public_key,
            private_key,
            chunk_size,
        }
    }

    fn encrypt_chunk(&self, chunk: &[u8]) -> Vec<u8> {
        if self.public_key.is_none() {
            panic!("Public key is required for encryption.");
        }
        let (e, n) = self.public_key.as_ref().unwrap();
        let chunk_int = BigUint::from_bytes_be(chunk);
        let encrypted_chunk_int = chunk_int.modpow(e, n);
        encrypted_chunk_int.to_bytes_be()
    }

    fn decrypt_chunk(&self, encrypted_chunk: &[u8]) -> Vec<u8> {
        if self.private_key.is_none() {
            panic!("Private key is required for decryption.");
        }
        let (d, n) = self.private_key.as_ref().unwrap();
        let encrypted_chunk_int = BigUint::from_bytes_be(encrypted_chunk);
        let decrypted_chunk_int = encrypted_chunk_int.modpow(d, n);
        let mut decrypted_chunk = decrypted_chunk_int.to_bytes_be();
        while decrypted_chunk.starts_with(&[0]) {
            decrypted_chunk.remove(0);
        }
        decrypted_chunk
    }

    pub fn encrypt_string(&self, plaintext: &str) -> String {
        if self.chunk_size == 0 {
            panic!("Public key required for encryption.");
        }
        let text_bytes = plaintext.as_bytes();
        let chunks = text_bytes.chunks(self.chunk_size);
        let encrypted_chunks: Vec<String> = chunks
            .map(|chunk| {
                let encrypted_chunk = self.encrypt_chunk(chunk);
                base64::encode(encrypted_chunk)
            })
            .collect();
        encrypted_chunks.join("|")
    }

    pub fn decrypt_string(&self, encrypted_data: &str) -> String {
        if self.private_key.is_none() {
            panic!("Private key required for decryption.");
        }
        let encrypted_chunks: Vec<Vec<u8>> = encrypted_data
            .split('|')
            .map(|chunk| base64::decode(chunk).expect("Failed to decode base64"))
            .collect();
        let decrypted_chunks: Vec<Vec<u8>> = encrypted_chunks
            .iter()
            .map(|chunk| self.decrypt_chunk(chunk))
            .collect();
        String::from_utf8(decrypted_chunks.concat()).expect("Failed to convert decrypted bytes to string")
    }
}

// fn main() {
//     // Example usage
//     let reader = PEMFileReader::new("path/to/your/public_key.pem");
//     let (e, n) = reader.load_public_pkcs8_key();
//     let rsa_encryptor = SimpleRSAChunkEncryptor::new(Some((e, n)), None);

//     let encrypted = rsa_encryptor.encrypt_string("Hello, world!");
//     println!("Encrypted: {}", encrypted);
// }
