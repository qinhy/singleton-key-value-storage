#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <stdexcept>
#include <cmath>
#include <tuple>
#include <InfInt.h>

class PEMFileReader {
public:
    PEMFileReader(const std::string& file_path) : file_path_(file_path) {
        key_bytes_ = read_pem_file();
    }

    std::tuple<InfInt, InfInt> load_public_key_from_pkcs8() {
        auto [data, _] = parse_asn1_der_sequence(key_bytes_, 0);
        size_t index = 0;

        // Parse algorithm identifier SEQUENCE (skip it)
        std::tie(std::ignore, index) = parse_asn1_der_sequence(data, index);

        // Parse BIT STRING
        auto [tag, _, value, new_index] = parse_asn1_der_element(data, index);
        if (tag != 0x03 || value[0] != 0x00) {
            throw std::runtime_error("Expected BIT STRING");
        }
        std::vector<uint8_t> public_key_bytes(value.begin() + 1, value.end());

        // Parse RSAPublicKey SEQUENCE
        auto [rsa_key_data, _] = parse_asn1_der_sequence(public_key_bytes, 0);
        index = 0;

        // Parse modulus (n) and exponent (e)
        InfInt n, e;
        std::tie(n, index) = parse_asn1_der_integer(rsa_key_data, index);
        std::tie(e, _) = parse_asn1_der_integer(rsa_key_data, index);

        return std::make_tuple(e, n);
    }

private:
    std::string file_path_;
    std::vector<uint8_t> key_bytes_;

    std::vector<uint8_t> read_pem_file() {
        std::ifstream file(file_path_);
        if (!file.is_open()) throw std::runtime_error("Cannot open file");

        std::string line, key_data;
        while (std::getline(file, line)) {
            if (line.find("BEGIN") == std::string::npos && line.find("END") == std::string::npos) {
                key_data += line;
            }
        }
        return base64_decode(key_data);
    }

    std::vector<uint8_t> base64_decode(const std::string& input) {
        // Implement base64 decoding here or use an external library function if allowed
        // Note: C++ does not provide built-in Base64 decoding in the standard library.
        return std::vector<uint8_t>(); // Placeholder for base64 decoding
    }

    std::tuple<uint8_t, size_t, std::vector<uint8_t>, size_t> parse_asn1_der_element(const std::vector<uint8_t>& data, size_t index) {
        uint8_t tag = data[index++];
        uint8_t length_byte = data[index++];

        size_t length;
        if ((length_byte & 0x80) == 0) {
            length = length_byte & 0x7F;
        } else {
            size_t num_length_bytes = length_byte & 0x7F;
            length = 0;
            for (size_t i = 0; i < num_length_bytes; ++i) {
                length = (length << 8) | data[index++];
            }
        }

        std::vector<uint8_t> value(data.begin() + index, data.begin() + index + length);
        index += length;

        return {tag, length, value, index};
    }

    std::tuple<InfInt, size_t> parse_asn1_der_integer(const std::vector<uint8_t>& data, size_t index) {
        auto [tag, _, value, new_index] = parse_asn1_der_element(data, index);
        if (tag != 0x02) throw std::runtime_error("Expected INTEGER");

        InfInt integer = 0;
        for (uint8_t byte : value) {
            integer = (integer * 256) + byte;
        }
        return {integer, new_index};
    }

    std::tuple<std::vector<uint8_t>, size_t> parse_asn1_der_sequence(const std::vector<uint8_t>& data, size_t index) {
        auto [tag, length, value, new_index] = parse_asn1_der_element(data, index);
        if (tag != 0x30) throw std::runtime_error("Expected SEQUENCE");

        return {value, new_index};
    }
};

class SimpleRSAChunkEncryptor {
public:
    SimpleRSAChunkEncryptor(std::tuple<InfInt, InfInt> public_key, std::tuple<InfInt, InfInt> private_key = {})
        : public_key_(public_key), private_key_(private_key) {
        InfInt n = std::get<1>(public_key_);
        chunk_size_ = (bit_length(n) / 8) - 1;
        if (chunk_size_ <= 0) {
            throw std::runtime_error("The modulus 'n' is too small. Please use a larger key size.");
        }
    }

    std::vector<uint8_t> encrypt_chunk(const std::vector<uint8_t>& chunk) {
        InfInt e = std::get<0>(public_key_);
        InfInt n = std::get<1>(public_key_);
        InfInt chunk_int = bytes_to_int(chunk);
        InfInt encrypted_chunk_int = mod_exp(chunk_int, e, n);
        return int_to_bytes(encrypted_chunk_int, (bit_length(n) + 7) / 8);
    }

    std::vector<uint8_t> decrypt_chunk(const std::vector<uint8_t>& encrypted_chunk) {
        InfInt d = std::get<0>(private_key_);
        InfInt n = std::get<1>(private_key_);
        InfInt encrypted_chunk_int = bytes_to_int(encrypted_chunk);
        InfInt decrypted_chunk_int = mod_exp(encrypted_chunk_int, d, n);
        auto decrypted_chunk = int_to_bytes(decrypted_chunk_int, (bit_length(n) + 7) / 8);
        return decrypted_chunk;
    }

private:
    std::tuple<InfInt, InfInt> public_key_;
    std::tuple<InfInt, InfInt> private_key_;
    int chunk_size_;

    int bit_length(const InfInt& value) {
        return value.numberOfDigits();
    }

    InfInt mod_exp(const InfInt& base, const InfInt& exp, const InfInt& mod) {
        InfInt result = 1;
        InfInt b = base;
        InfInt e = exp;
        while (e > 0) {
            if (e % 2 == 1) result = (result * b) % mod;
            b = (b * b) % mod;
            e /= 2;
        }
        return result;
    }

    InfInt bytes_to_int(const std::vector<uint8_t>& bytes) {
        InfInt result = 0;
        for (uint8_t byte : bytes) {
            result = (result * 256) + byte;
        }
        return result;
    }

    std::vector<uint8_t> int_to_bytes(InfInt value, size_t size) {
        std::vector<uint8_t> bytes(size);
        for (size_t i = size; i > 0; --i) {
            bytes[i - 1] = static_cast<uint8_t>(value.toInt() & 0xFF);
            value /= 256;
        }
        return bytes;
    }
};
