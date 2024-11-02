#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <stdexcept>
#include <cmath>
#include <tuple>
#include <InfInt.h>

namespace base64
{

    static const std::string chars =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789+/";

    inline bool is_base64(unsigned char c)
    {
        return (c == 43 ||              // '+'
                (c >= 47 && c <= 57) || // '0'-'9'
                (c >= 65 && c <= 90) || // 'A'-'Z'
                (c >= 97 && c <= 122)); // 'a'-'z'
    }

    // Encode a vector of bytes (uint8_t) to a base64 string
    inline std::string encode(const std::vector<uint8_t> &input)
    {
        std::string output;
        int i = 0;
        unsigned char char_array_3[3];
        unsigned char char_array_4[4];

        size_t len = input.size();
        auto it = input.begin();

        while (len--)
        {
            char_array_3[i++] = *(it++);
            if (i == 3)
            {
                char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
                char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
                char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
                char_array_4[3] = char_array_3[2] & 0x3f;

                for (i = 0; i < 4; i++)
                {
                    output += chars[char_array_4[i]];
                }
                i = 0;
            }
        }

        if (i > 0)
        {
            for (int j = i; j < 3; j++)
                char_array_3[j] = '\0';

            char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
            char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
            char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);

            for (int j = 0; j < i + 1; j++)
            {
                output += chars[char_array_4[j]];
            }

            while (i++ < 3)
                output += '=';
        }

        return output;
    }

    // Decode a base64 string into a vector of bytes (uint8_t)
    inline std::vector<uint8_t> decode(const std::string &input)
    {
        size_t len = input.size();
        int i = 0, in = 0;
        unsigned char char_array_4[4], char_array_3[3];
        std::vector<uint8_t> output;

        while (len-- && (input[in] != '=') && is_base64(input[in]))
        {
            char_array_4[i++] = input[in++];
            if (i == 4)
            {
                for (i = 0; i < 4; i++)
                {
                    char_array_4[i] = static_cast<unsigned char>(chars.find(char_array_4[i]));
                }

                char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
                char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
                char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

                for (i = 0; i < 3; i++)
                    output.push_back(char_array_3[i]);
                i = 0;
            }
        }

        if (i > 0)
        {
            for (int j = i; j < 4; j++)
                char_array_4[j] = 0;

            for (int j = 0; j < 4; j++)
            {
                char_array_4[j] = static_cast<unsigned char>(chars.find(char_array_4[j]));
            }

            char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
            char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
            char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

            for (int j = 0; j < i - 1; j++)
                output.push_back(char_array_3[j]);
        }

        return output;
    }

} // namespace base64

class PEMFileReader
{
public:
    PEMFileReader(const std::string &file_path) : file_path_(file_path)
    {
        key_bytes_ = read_pem_file();
    }
    

    // Load Public Key and convert components to InfInt
    std::tuple<InfInt, InfInt> load_public_key_from_pkcs8() {
        // Get the modulus and exponent as vectors of uint8_t
        auto [e_vec, n_vec] = load_public_key_from_pkcs8_vec_uint8();

        // Convert vectors to InfInt
        InfInt e = vector_to_infint(e_vec);
        InfInt n = vector_to_infint(n_vec);

        return std::make_tuple(e, n);
    }

    // Load Private Key and convert components to InfInt
    std::tuple<InfInt, InfInt> load_private_key_from_pkcs8() {
        // Get the modulus and private exponent as vectors of uint8_t
        auto [d_vec, n_vec] = load_private_key_from_pkcs8_vec_uint8();

        // Convert vectors to InfInt
        InfInt d = vector_to_infint(d_vec);
        InfInt n = vector_to_infint(n_vec);

        return std::make_tuple(d, n);
    }

    std::tuple<std::vector<uint8_t>, std::vector<uint8_t>> load_public_key_from_pkcs8_vec_uint8()
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

    std::tuple<std::vector<uint8_t>, std::vector<uint8_t>> load_private_key_from_pkcs8_vec_uint8()
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

    // Helper function to convert a vector<uint8_t> to InfInt
    InfInt vector_to_infint(const std::vector<uint8_t>& vec) {
        InfInt result = 0;
        for (uint8_t byte : vec) {
            result = (result * 256) + byte;
        }
        return result;
    }

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


class SimpleRSAChunkEncryptor
{
public:
    SimpleRSAChunkEncryptor(std::tuple<InfInt, InfInt> public_key, std::tuple<InfInt, InfInt> private_key = {})
        : public_key_(public_key), private_key_(private_key)
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
        std::vector<uint8_t> bytes(size);
        for (size_t i = size; i > 0; --i)
        {
            bytes[i - 1] = static_cast<uint8_t>(value.toInt() & 0xFF);
            value /= 256;
        }
        return bytes;
    }
};
