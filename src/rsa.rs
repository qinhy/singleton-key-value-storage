use base64;
use num_bigint::BigUint;
use num_traits::ToPrimitive;
use std::fs::File;
use std::io::{BufRead, BufReader, Error, ErrorKind, Result};

pub struct PEMFileReader {
    key_bytes: Vec<u8>,
}

impl PEMFileReader {
    pub fn new(file_path: &str) -> Result<Self> {
        let key_bytes = Self::read_pem_file(file_path)?;
        Ok(Self { key_bytes })
    }

    fn read_pem_file(file_path: &str) -> Result<Vec<u8>> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let mut key_data = String::new();
        for line in reader.lines() {
            let line = line?;
            if !line.starts_with("-----BEGIN") && !line.starts_with("-----END") {
                key_data.push_str(&line);
            }
        }
        base64::decode(key_data)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("Base64 decode error: {}", e)))
    }

    fn parse_asn1_der_element(data: &[u8], index: &mut usize) -> Result<(u8, usize, Vec<u8>)> {
        if *index >= data.len() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "Reached end of data"));
        }
        let tag = data[*index];
        *index += 1;

        if *index >= data.len() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "Reached end of data"));
        }
        let length_byte = data[*index];
        *index += 1;

        let length = if length_byte & 0x80 == 0 {
            (length_byte & 0x7F) as usize
        } else {
            let num_length_bytes = (length_byte & 0x7F) as usize;
            if *index + num_length_bytes > data.len() {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "Insufficient bytes for length",
                ));
            }
            let length = BigUint::from_bytes_be(&data[*index..*index + num_length_bytes])
                .to_usize()
                .ok_or_else(|| Error::new(ErrorKind::InvalidData, "Failed to convert length"))?;
            *index += num_length_bytes;
            length
        };

        if *index + length > data.len() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "Insufficient bytes for value",
            ));
        }
        let value = data[*index..*index + length].to_vec();
        *index += length;

        Ok((tag, length, value))
    }

    fn parse_asn1_der_integer(data: &[u8], index: &mut usize) -> Result<BigUint> {
        let (tag, _, value) = Self::parse_asn1_der_element(data, index)?;
        if tag != 0x02 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Expected INTEGER tag, found different tag",
            ));
        }
        Ok(BigUint::from_bytes_be(&value))
    }

    fn parse_asn1_der_sequence(data: &[u8], index: &mut usize) -> Result<Vec<u8>> {
        let (tag, _, value) = Self::parse_asn1_der_element(data, index)?;
        if tag != 0x30 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Expected SEQUENCE tag, found different tag",
            ));
        }
        Ok(value)
    }

    pub fn load_public_pkcs8_key(&self) -> Result<(BigUint, BigUint)> {
        let mut index = 0;
        let data = Self::parse_asn1_der_sequence(&self.key_bytes, &mut index)?;

        let mut index = 0;
        let _algorithm_id = Self::parse_asn1_der_sequence(&data, &mut index)?;

        let (tag, _, value) = Self::parse_asn1_der_element(&data, &mut index)?;
        if tag != 0x03 {
            return Err(Error::new(ErrorKind::InvalidData, "Expected BIT STRING tag"));
        }
        if value[0] != 0x00 {
            return Err(Error::new(ErrorKind::InvalidData, "Invalid BIT STRING padding"));
        }
        let public_key_bytes = &value[1..];

        let mut index = 0;
        let rsa_key_data = Self::parse_asn1_der_sequence(public_key_bytes, &mut index)?;

        let mut index = 0;
        let n = Self::parse_asn1_der_integer(&rsa_key_data, &mut index)?;
        let e = Self::parse_asn1_der_integer(&rsa_key_data, &mut index)?;

        Ok((e, n))
    }

    pub fn load_private_pkcs8_key(&self) -> Result<(BigUint, BigUint)> {
        let mut index = 0;
        let data = Self::parse_asn1_der_sequence(&self.key_bytes, &mut index)?;

        let mut index = 0;
        let _version = Self::parse_asn1_der_integer(&data, &mut index)?;

        let _algorithm_id = Self::parse_asn1_der_sequence(&data, &mut index)?;

        let (tag, _, private_key_bytes) = Self::parse_asn1_der_element(&data, &mut index)?;
        if tag != 0x04 {
            return Err(Error::new(ErrorKind::InvalidData, "Expected OCTET STRING tag"));
        }

        let mut index = 0;
        let rsa_key_data = Self::parse_asn1_der_sequence(&private_key_bytes, &mut index)?;

        let mut index = 0;
        let _version = Self::parse_asn1_der_integer(&rsa_key_data, &mut index)?;

        let n = Self::parse_asn1_der_integer(&rsa_key_data, &mut index)?;
        Self::parse_asn1_der_integer(&rsa_key_data, &mut index)?;
        let d = Self::parse_asn1_der_integer(&rsa_key_data, &mut index)?;

        Ok((d, n))
    }
}

pub struct SimpleRSAChunkEncryptor {
    public_key: Option<(BigUint, BigUint)>,
    private_key: Option<(BigUint, BigUint)>,
    chunk_size: usize,
}

impl SimpleRSAChunkEncryptor {
    pub fn new(public_key: Option<(BigUint, BigUint)>, private_key: Option<(BigUint, BigUint)>) -> Result<Self> {
        let chunk_size = if let Some((_, n)) = &public_key {
            (n.bits() as usize / 8) - 1
        } else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Public key is required for encryption",
            ));
        };

        if chunk_size <= 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "The modulus 'n' is too small. Please use a larger key size.",
            ));
        }

        Ok(Self {
            public_key,
            private_key,
            chunk_size,
        })
    }

    fn encrypt_chunk(&self, chunk: &[u8]) -> Result<Vec<u8>> {
        if let Some((e, n)) = &self.public_key {
            let chunk_int = BigUint::from_bytes_be(chunk);
            let encrypted_chunk_int = chunk_int.modpow(e, n);
            Ok(encrypted_chunk_int.to_bytes_be())
        } else {
            Err(Error::new(
                ErrorKind::InvalidInput,
                "Public key is required for encryption",
            ))
        }
    }

    fn decrypt_chunk(&self, encrypted_chunk: &[u8]) -> Result<Vec<u8>> {
        if let Some((d, n)) = &self.private_key {
            let encrypted_chunk_int = BigUint::from_bytes_be(encrypted_chunk);
            let decrypted_chunk_int = encrypted_chunk_int.modpow(d, n);
            let mut decrypted_chunk = decrypted_chunk_int.to_bytes_be();
            while decrypted_chunk.starts_with(&[0]) {
                decrypted_chunk.remove(0);
            }
            Ok(decrypted_chunk)
        } else {
            Err(Error::new(
                ErrorKind::InvalidInput,
                "Private key is required for decryption",
            ))
        }
    }

    pub fn encrypt_string(&self, plaintext: &str) -> Result<String> {
        if self.chunk_size == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Public key required for encryption",
            ));
        }
        let text_bytes = plaintext.as_bytes();
        let chunks = text_bytes.chunks(self.chunk_size);
        let encrypted_chunks: Result<Vec<String>> = chunks
            .map(|chunk| {
                let encrypted_chunk = self.encrypt_chunk(chunk)?;
                Ok(base64::encode(encrypted_chunk))
            })
            .collect();
        Ok(encrypted_chunks?.join("|"))
    }

    pub fn decrypt_string(&self, encrypted_data: &str) -> Result<String> {
        if self.private_key.is_none() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Private key required for decryption",
            ));
        }
        let encrypted_chunks: Result<Vec<Vec<u8>>> = encrypted_data
            .split('|')
            .map(|chunk| {
                base64::decode(chunk).map_err(|e| {
                    Error::new(ErrorKind::InvalidData, format!("Failed to decode base64: {}", e))
                })
            })
            .collect();
        let decrypted_chunks: Result<Vec<Vec<u8>>> = encrypted_chunks?
            .iter()
            .map(|chunk| self.decrypt_chunk(chunk))
            .collect();
        let decrypted_bytes = decrypted_chunks?.concat();
        String::from_utf8(decrypted_bytes)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("UTF-8 error: {}", e)))
    }
}
