/**
    @file
    @author Alexander Sherikov

    @copyright 2025 Alexander Sherikov. Licensed under the Apache License,
    Version 2.0. (see LICENSE or http://www.apache.org/licenses/LICENSE-2.0)

    @brief
*/

#include <regex>
#include <random>
#include <filesystem>

#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/container/flat_map.hpp>

#include "HeaderCdrAux.ipp"
#include "StatisticsNamesCdrAux.ipp"
#include "StatisticsValuesCdrAux.ipp"
#include "TimeCdrAux.ipp"

#define MCAP_IMPLEMENTATION
#define MCAP_COMPRESSION_NO_LZ4
#define MCAP_COMPRESSION_NO_ZSTD
#define MCAP_PUBLIC __attribute__((visibility("hidden")))

#pragma GCC diagnostic push
// presumably GCC bug
#pragma GCC diagnostic ignored "-Warray-bounds"
#include <mcap/writer.hpp>
#pragma GCC diagnostic pop

#include "messages.h"


namespace
{
    class Parameters
    {
    public:
        /// graphite input ip address
        std::string ip_;
        /// graphite input port
        std::string port_;
        /// drop prefix of the given length, e.g., to eliminate `Hostname`
        /// configuration parameter that precedes all metric names
        std::size_t skip_prefix_len_;
        /// names of the metrics that need to be converted to percents
        std::vector<std::regex> microsecond_to_percent_regex_;

    public:
        Parameters()
        {
            ip_ = "127.0.0.1";
            port_ = "2003";
            skip_prefix_len_ = 2;

            microsecond_to_percent_regex_.clear();
            microsecond_to_percent_regex_.emplace_back(".*processes.*ps_cputime\\.user");
            microsecond_to_percent_regex_.emplace_back(".*processes.*ps_cputime\\.syst");
        }
    };


    class McapCDRWriter
    {
    protected:
        template <class t_Message>
        class Channel
        {
        protected:
            mcap::Message message_;
            eprosima::fastcdr::CdrSizeCalculator cdr_size_calculator_;

        protected:
            uint32_t getSize(const t_Message &message)
            {
                size_t current_alignment{ 0 };
                return (cdr_size_calculator_.calculate_serialized_size(message, current_alignment)
                        + 4u /*encapsulation*/);
            }

        public:
            Channel() : cdr_size_calculator_(eprosima::fastcdr::CdrVersion::XCDRv1)
            {
            }

            void initialize(mcap::McapWriter &writer, const std::string_view &msg_topic)
            {
                mcap::Schema schema(
                        intrometry_private::pjmsg_mcap::Message<t_Message>::type,
                        "ros2msg",
                        intrometry_private::pjmsg_mcap::Message<t_Message>::schema);
                writer.addSchema(schema);

                mcap::Channel channel(msg_topic, "ros2msg", schema.id);
                writer.addChannel(channel);

                message_.channelId = channel.id;
            }

            void write(mcap::McapWriter &writer, std::vector<std::byte> &buffer, const t_Message &message)
            {
                buffer.resize(getSize(message));
                message_.data = buffer.data();

                {
                    eprosima::fastcdr::FastBuffer cdr_buffer(
                            reinterpret_cast<char *>(buffer.data()), buffer.size());  // NOLINT
                    eprosima::fastcdr::Cdr ser(
                            cdr_buffer, eprosima::fastcdr::Cdr::DEFAULT_ENDIAN, eprosima::fastcdr::CdrVersion::XCDRv1);
                    ser.set_encoding_flag(eprosima::fastcdr::EncodingAlgorithmFlag::PLAIN_CDR);

                    ser.serialize_encapsulation();
                    ser << message;
                    ser.set_dds_cdr_options({ 0, 0 });

                    message_.dataSize = ser.get_serialized_data_length();
                }

                message_.logTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                           std::chrono::system_clock::now().time_since_epoch())
                                           .count();
                message_.publishTime = message_.logTime;


                const mcap::Status res = writer.write(message_);
                if (not res.ok())
                {
                    throw std::runtime_error(res.message);
                }
            }
        };

    public:
        std::tuple<Channel<plotjuggler_msgs::msg::StatisticsNames>, Channel<plotjuggler_msgs::msg::StatisticsValues>>
                channels_;

        std::vector<std::byte> buffer_;
        mcap::McapWriter writer_;

    public:
        ~McapCDRWriter()
        {
            writer_.close();
        }

        void initialize(const std::filesystem::path &filename, const std::string &topic_prefix)
        {
            {
                const mcap::McapWriterOptions options = mcap::McapWriterOptions("ros2msg");
                const mcap::Status res = writer_.open(filename.native(), options);
                if (not res.ok())
                {
                    throw std::runtime_error(res.message);
                }
            }

            std::get<Channel<plotjuggler_msgs::msg::StatisticsNames>>(channels_).initialize(
                    writer_, topic_prefix + "/names");

            std::get<Channel<plotjuggler_msgs::msg::StatisticsValues>>(channels_).initialize(
                    writer_, topic_prefix + "/values");
        }

        template <class t_Message>
        void write(const t_Message &message)
        {
            std::get<Channel<t_Message>>(channels_).write(writer_, buffer_, message);
        }

        void flush()
        {
            writer_.closeLastChunk();
            writer_.dataSink()->flush();
        }
    };


    class MessageWriter
    {
    public:
        using DataContainer = boost::container::flat_map<std::string, double>;

    public:
        DataContainer data_;
        plotjuggler_msgs::msg::StatisticsNames names_;
        plotjuggler_msgs::msg::StatisticsValues values_;
        int32_t prev_timestamp_;
        uint32_t names_version_;
        bool version_updated_;
        McapCDRWriter mcap_writer_;

    public:
        MessageWriter()
        {
            prev_timestamp_ = 0;
            names_version_ = getRandomUInt32();
            updateVersion();
            names_.names().reserve(500);
            values_.values().reserve(500);
            mcap_writer_.initialize("graphite_to_mcap.mcap", "/graphite_to_mcap");
        }

        void add(const std::string &name, const double value, const int32_t time)
        {
            if (names_.header().stamp().sec() != time)
            {
                if (not data_.empty())
                {
                    if (data_.size() != names_.names().size())
                    {
                        updateVersion();
                        resize(data_.size());
                    }

                    std::size_t index = 0;
                    for (const DataContainer::value_type & entry : data_)
                    {
                        if (not version_updated_ and entry.first != names_.names()[index])
                        {
                            updateVersion();
                        }
                        if (version_updated_)
                        {
                            names_.names()[index] = entry.first;
                        }
                        values_.values()[index] = entry.second;

                        ++index;
                    }

                    if (version_updated_)
                    {
                        mcap_writer_.write(names_);
                        version_updated_ = false;
                    }
                    mcap_writer_.write(values_);
                    mcap_writer_.flush();

                    data_.clear();
                }

                prev_timestamp_ = names_.header().stamp().sec();
                names_.header().stamp().sec(time);
                values_.header().stamp().sec(time);
            }


            data_[name] = value;
        }

    protected:
        static uint32_t getRandomUInt32()
        {
            std::mt19937 gen((std::random_device())());

            std::uniform_int_distribution<uint32_t> distrib(
                    std::numeric_limits<uint32_t>::min(), std::numeric_limits<uint32_t>::max());

            return (distrib(gen));
        }

        void updateVersion()
        {
            if (not version_updated_)
            {
                ++names_version_;
                names_.names_version(names_version_);
                values_.names_version(names_version_);
                version_updated_ = true;
            }
        }

        void resize(const std::size_t new_size)
        {
            names_.names().resize(new_size);
            values_.values().resize(new_size);
        }
    };
}  // namespace


int main(/*int argc, char **argv*/)
{
    try
    {
        const Parameters parameters;

        boost::asio::io_service io_service;

        boost::asio::ip::tcp::resolver resolver(io_service);
        const boost::asio::ip::tcp::resolver::query query(parameters.ip_, parameters.port_);
        const boost::asio::ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

        boost::asio::ip::tcp::acceptor acceptor(io_service, endpoint_iterator->endpoint());

        boost::asio::ip::tcp::socket socket(io_service);
        acceptor.accept(socket);

        boost::asio::streambuf buffer;



        std::string line;
        std::string name;

        MessageWriter message_writer;

        for (;;)
        {
            {
                boost::system::error_code error;

                boost::asio::read_until(socket, buffer, "\n", error);
                switch (error.value())
                {
                    case boost::system::errc::success:
                        break;
                    case boost::asio::error::eof:
                        throw std::runtime_error("Connection closed.");
                    default:
                        throw std::runtime_error(error.message());
                }
            }

            line.clear();
            std::getline(std::istream(&buffer), line);

            if (not line.empty())
            {
                std::string::size_type token_start = 0;
                std::string::size_type token_end = line.find(' ', token_start);

                bool percent_flag = false;
                {
                    if ((std::string::npos == token_end) or (token_start >= token_end))
                    {
                        throw std::runtime_error("Cannot read metric name.");
                    }
                    if (token_end - token_start > parameters.skip_prefix_len_)
                    {
                        name = line.substr(
                                token_start + parameters.skip_prefix_len_, token_end - parameters.skip_prefix_len_);
                    }
                    else
                    {
                        name = line.substr(token_start, token_end);
                    }
                    for (const std::regex &match_pattern : parameters.microsecond_to_percent_regex_)
                    {
                        if (std::regex_match(name, match_pattern))
                        {
                            percent_flag = true;
                            break;
                        }
                    }
                    token_start = token_end + 1;
                }

                token_end = line.find(' ', token_start);
                if ((std::string::npos == token_end) or (token_start >= token_end))
                {
                    throw std::runtime_error("Cannot read metric value.");
                }
                const double value = boost::lexical_cast<double>(line.substr(token_start, token_end - token_start));
                token_start = token_end + 1;


                if (token_start >= line.size())
                {
                    throw std::runtime_error("Cannot read metric stamp.");
                }
                const int32_t timestamp =
                        boost::lexical_cast<int32_t>(line.substr(token_start, line.size() - token_start - 1));


                message_writer.add(name, value, timestamp);
                if (percent_flag)  // microseconds to percent
                {
                    double percent_value = 0.0;
                    if (message_writer.prev_timestamp_ > 0 && message_writer.prev_timestamp_ < timestamp)
                    {
                        // (metric_value / (time_in_seconds * 1000000)) * 100
                        percent_value = value / ((timestamp - message_writer.prev_timestamp_) * 10000);
                    }
                    message_writer.add(name + "_percent", percent_value, timestamp);
                }
            }
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Exception: " << e.what() << std::endl;  // NOLINT
        return (EXIT_FAILURE);
    }

    return (EXIT_SUCCESS);
}
