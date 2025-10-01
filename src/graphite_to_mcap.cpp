/**
    @file
    @author Alexander Sherikov

    @copyright 2025 Alexander Sherikov. Licensed under the Apache License,
    Version 2.0. (see LICENSE or http://www.apache.org/licenses/LICENSE-2.0)

    @brief
*/

#include <pjmsg_mcap_wrapper/all.h>

#include <regex>
#include <iostream>

#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/container/flat_map.hpp>


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


    class MessageWriter
    {
    public:
        using DataContainer = boost::container::flat_map<std::string, double>;

    public:
        DataContainer data_;
        uint64_t current_timestamp_;
        uint64_t prev_timestamp_;
        pjmsg_mcap_wrapper::Message message_;
        pjmsg_mcap_wrapper::Writer mcap_writer_;

    public:
        MessageWriter()
        {
            current_timestamp_ = 0;
            prev_timestamp_ = 0;
            mcap_writer_.initialize("graphite_to_mcap.mcap", "/graphite_to_mcap");
            message_.reserve(500);
        }

        void add(const std::string &name, const double value, const uint64_t timestamp)
        {
            if (current_timestamp_ != timestamp)
            {
                if (not data_.empty())
                {
                    if (data_.size() != message_.size())
                    {
                        message_.bumpVersion();
                        message_.resize(data_.size());
                    }

                    std::size_t index = 0;
                    bool version_updated = false;;
                    for (const DataContainer::value_type &entry : data_)
                    {
                        if (entry.first != message_.names()[index])
                        {
                            message_.bumpVersion();
                            version_updated = true;
                        }
                        if (version_updated)
                        {
                            message_.names()[index] = entry.first;
                        }
                        message_.values()[index] = entry.second;

                        ++index;
                    }

                    mcap_writer_.write(message_);
                    mcap_writer_.flush();

                    data_.clear();
                }

                prev_timestamp_ = current_timestamp_;
                current_timestamp_ = timestamp;
                message_.setStamp(timestamp);
            }

            data_[name] = value;
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
                const uint64_t timestamp =
                        boost::lexical_cast<uint64_t>(line.substr(token_start, line.size() - token_start - 1))
                        * std::nano::den;


                message_writer.add(name, value, timestamp);
                if (percent_flag)  // microseconds to percent
                {
                    double percent_value = 0.0;
                    if (message_writer.prev_timestamp_ > 0 && message_writer.prev_timestamp_ < timestamp)
                    {
                        // (metric_value / (duration_in_nanoseconds / 1000)) * 100
                        const uint64_t factor = (timestamp - message_writer.prev_timestamp_) / 100;
                        percent_value = value / static_cast<double>(factor);
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
