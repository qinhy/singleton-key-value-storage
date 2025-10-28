#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <stdexcept>
#include <cmath>
#include <tuple>
#include <InfInt.h>
#include <base64.hpp>


class PEMFileReader
{
public:
    PEMFileReader(const std::string &file_path) : file_path_(file_path)
    {
        key_bytes_ = read_pem_file();
    }
    
    std::tuple<std::vector<uint8_t>, std::vector<uint8_t>> load_public_key_from_pkcs8()
    {
        auto [data, data_index] = parse_asn1_der_sequence(key_bytes_, 0);
        size_t index = 0;

        // Parse algorithm identifier SEQUENCE (skip it)
        std::tie(std::ignore, index) = parse_asn1_der_sequence(data, index);

        // Parse BIT STRING
        auto [tag, bit_string_length, value, new_index] = parse_asn1_der_element(data, index);
        if (tag != 0x03 || value[0] != 0x00)
        {
            throw std::runtime_error("Expected BIT STRING");
        }
        std::vector<uint8_t> public_key_bytes(value.begin() + 1, value.end());

        // Parse RSAPublicKey SEQUENCE
        auto [rsa_key_data, rsa_key_data_index] = parse_asn1_der_sequence(public_key_bytes, 0);
        index = 0;

        // Parse modulus (n) and exponent (e)
        std::vector<uint8_t> n, e;
        std::tie(n, index) = parse_asn1_der_integer(rsa_key_data, index);
        std::tie(e, index) = parse_asn1_der_integer(rsa_key_data, index);

        return std::make_tuple(e, n);
    }

    std::tuple<std::vector<uint8_t>, std::vector<uint8_t>> load_private_key_from_pkcs8()
    {
        auto [data, data_index] = parse_asn1_der_sequence(key_bytes_, 0);
        size_t index = 0;

        // Parse version INTEGER (skip it)
        std::tie(std::ignore, index) = parse_asn1_der_integer(data, index);

        // Parse algorithm identifier SEQUENCE (skip it)
        std::tie(std::ignore, index) = parse_asn1_der_sequence(data, index);

        // Parse privateKey OCTET STRING
        auto [tag, octet_length, private_key_bytes, new_index] = parse_asn1_der_element(data, index);
        if (tag != 0x04)
        {
            throw std::runtime_error("Expected OCTET STRING");
        }

        // Parse RSAPrivateKey SEQUENCE
        auto [rsa_key_data, rsa_key_data_index] = parse_asn1_der_sequence(private_key_bytes, 0);
        index = 0;

        // Parse version INTEGER (skip it)
        std::tie(std::ignore, index) = parse_asn1_der_integer(rsa_key_data, index);

        // Parse modulus (n), publicExponent (e), and privateExponent (d)
        std::vector<uint8_t> n, e, d;
        std::tie(n, index) = parse_asn1_der_integer(rsa_key_data, index);
        std::tie(e, index) = parse_asn1_der_integer(rsa_key_data, index);
        std::tie(d, index) = parse_asn1_der_integer(rsa_key_data, index);

        return std::make_tuple(d, n);
    }

private:
    std::string file_path_;
    std::vector<uint8_t> key_bytes_;

    std::vector<uint8_t> read_pem_file()
    {
        std::ifstream file(file_path_);
        if (!file.is_open())
            throw std::runtime_error("Cannot open file");

        std::string line, key_data;
        while (std::getline(file, line))
        {
            if (line.find("BEGIN") == std::string::npos && line.find("END") == std::string::npos)
            {
                key_data += line;
            }
        }
        return base64::decode(key_data);
    }

    std::tuple<uint8_t, size_t, std::vector<uint8_t>, size_t> parse_asn1_der_element(const std::vector<uint8_t> &data, size_t index)
    {
        uint8_t tag = data[index++];
        uint8_t length_byte = data[index++];

        size_t length;
        if ((length_byte & 0x80) == 0)
        {
            length = length_byte & 0x7F;
        }
        else
        {
            size_t num_length_bytes = length_byte & 0x7F;
            length = 0;
            for (size_t i = 0; i < num_length_bytes; ++i)
            {
                length = (length << 8) | data[index++];
            }
        }

        std::vector<uint8_t> value(data.begin() + index, data.begin() + index + length);
        index += length;

        return {tag, length, value, index};
    }


    std::tuple<std::vector<uint8_t>, size_t> parse_asn1_der_integer(const std::vector<uint8_t>& data, size_t index) {
        auto [tag, _, value, new_index] = parse_asn1_der_element(data, index);
        if (tag != 0x02) {
            throw std::runtime_error("Expected INTEGER");
        }
        return {value, new_index};
    }

    std::tuple<std::vector<uint8_t>, size_t> parse_asn1_der_sequence(const std::vector<uint8_t> &data, size_t index)
    {
        auto [tag, length, value, new_index] = parse_asn1_der_element(data, index);
        if (tag != 0x30)
            throw std::runtime_error("Expected SEQUENCE");

        return {value, new_index};
    }
};


std::string format_as_bytes(const std::vector<uint8_t>& data) {
    std::ostringstream oss;
    oss << "b\""; // Start with `b"`

    for (uint8_t byte : data) {
        oss << "\\x" << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(byte);
    }

    oss << "\""; // End with `"`
    return oss.str();
}


class SimpleRSAChunkEncryptor
{
public:
    SimpleRSAChunkEncryptor(std::tuple<std::vector<uint8_t>, std::vector<uint8_t>> public_key, std::tuple<std::vector<uint8_t>, std::vector<uint8_t>> private_key = {})
        : public_key_(std::make_tuple(bytes_to_int(std::get<0>(public_key)),bytes_to_int(std::get<1>(public_key)))), 
          private_key_((std::make_tuple(bytes_to_int(std::get<0>(private_key)),bytes_to_int(std::get<1>(private_key)))))
    {
        InfInt n = std::get<1>(public_key_);
        chunk_size_ = (bit_length(n) / 8) - 1;
        if (chunk_size_ <= 0)
        {
            throw std::runtime_error("The modulus 'n' is too small. Please use a larger key size.");
        }
    }

    std::vector<uint8_t> encrypt_chunk(const std::vector<uint8_t> &chunk)
    {
        InfInt e = std::get<0>(public_key_);
        InfInt n = std::get<1>(public_key_);
        InfInt chunk_int = bytes_to_int(chunk);
        InfInt encrypted_chunk_int = mod_exp(chunk_int, e, n);
        return int_to_bytes(encrypted_chunk_int, (bit_length(n) + 7) / 8);
    }

    std::vector<uint8_t> decrypt_chunk(const std::vector<uint8_t> &encrypted_chunk)
    {
        InfInt d = std::get<0>(private_key_);
        InfInt n = std::get<1>(private_key_);
        InfInt encrypted_chunk_int = bytes_to_int(encrypted_chunk);
        InfInt decrypted_chunk_int = mod_exp(encrypted_chunk_int, d, n);
        auto decrypted_chunk = int_to_bytes(decrypted_chunk_int, (bit_length(n) + 7) / 8);
        return decrypted_chunk;
    }

    std::string encrypt_string(const std::string &plaintext)
    {
        std::vector<uint8_t> text_bytes(plaintext.begin(), plaintext.end());
        std::vector<std::string> encoded_chunks;

        for (size_t i = 0; i < text_bytes.size(); i += chunk_size_)
        {
            std::vector<uint8_t> chunk(text_bytes.begin() + i, text_bytes.begin() + std::min(i + chunk_size_, text_bytes.size()));
            std::vector<uint8_t> encrypted_chunk = encrypt_chunk(chunk);
            encoded_chunks.push_back(base64::encode(encrypted_chunk));
        }

        std::ostringstream oss;
        std::copy(encoded_chunks.begin(), encoded_chunks.end(), std::ostream_iterator<std::string>(oss, "|"));
        std::string encrypted_string = oss.str();
        encrypted_string.pop_back(); // Remove trailing '|'
        return encrypted_string;
    }

    std::string decrypt_string(const std::string &encrypted_data)
    {
        std::vector<std::string> chunks;
        std::istringstream iss(encrypted_data);
        std::string token;
        while (std::getline(iss, token, '|'))
        {
            chunks.push_back(token);
        }

        std::vector<uint8_t> decrypted_bytes;
        for (const auto &chunk : chunks)
        {
            std::vector<uint8_t> encrypted_chunk = base64::decode(chunk);
            std::vector<uint8_t> decrypted_chunk = decrypt_chunk(encrypted_chunk);
            decrypted_bytes.insert(decrypted_bytes.end(), decrypted_chunk.begin(), decrypted_chunk.end());
        }

        return std::string(decrypted_bytes.begin(), decrypted_bytes.end());
    }

private:
    std::tuple<InfInt, InfInt> public_key_;
    std::tuple<InfInt, InfInt> private_key_;
    int chunk_size_;

    int bit_length(const InfInt &value)
    {
        return value.numberOfDigits();
    }

    InfInt mod_exp(const InfInt &base, const InfInt &exp, const InfInt &mod)
    {
        InfInt result = 1;
        InfInt b = base;
        InfInt e = exp;
        while (e > 0)
        {
            if (e % 2 == 1)
                result = (result * b) % mod;
            b = (b * b) % mod;
            e /= 2;
        }
        return result;
    }

    InfInt bytes_to_int(const std::vector<uint8_t> &bytes)
    {
        InfInt result = 0;
        for (uint8_t byte : bytes)
        {
            result = (result * 256) + byte;
        }
        return result;
    }

    std::vector<uint8_t> int_to_bytes(InfInt value, size_t size)
    {
        return value.to_bytes();
    }
};
