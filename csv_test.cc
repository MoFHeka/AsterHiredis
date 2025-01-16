#include <string>
#include <charconv>
#include <memory>
#include <iostream>
#include <vector>

using namespace std;

string text = "# Replication\r\nrole:master\r\nconnected_slaves:1\r\nslave0:ip=127.0.0.1,port=7777,state=online,offset=1363189522,lag=1\r\nslave1:ip=127.0.0.1,port=1234,state=online,offset=1363189522,lag=1\r\nmaster_replid:beb22f8c18704f7467abda8a4509abf717269ad9\r\nmaster_replid2:0000000000000000000000000000000000000000\r\nmaster_repl_offset:1363189522\r\nsecond_repl_offset:-1\r\nrepl_backlog_active:1\r\nrepl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:1362140947\r\nrepl_backlog_histlen:1048576\r\n";

string slave_text = "# Replication\r\nrole:slave\r\nmaster_host:127.0.0.1\r\nmaster_port:6379\r\nmaster_link_status:up\r\nmaster_last_io_seconds_ago:2\r\nmaster_sync_in_progress:0\r\nslave_repl_offset:1363248504\r\nslave_priority:100\r\nslave_read_only:1\r\nconnected_slaves:0\r\nmaster_replid:beb22f8c18704f7467abda8a4509abf717269ad9\r\nmaster_replid2:0000000000000000000000000000000000000000\r\nmaster_repl_offset:1363248504\r\nsecond_repl_offset:-1\r\nrepl_backlog_active:1\r\nrepl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:1362199929\r\nrepl_backlog_histlen:1048576";

int main(int argc, char **argv) {
  size_t found = 0;
  std::vector<std::pair<std::string, uint16_t>> slave_ip_port_pair;
  std::pair<std::string, uint16_t> master_ip_port_pair;
  size_t ip_start = 0, ip_end = 0, port_start = 0, port_end = 0;

  size_t role_start = 0, role_end = 0;
  found = 15;
  found = slave_text.find("role:",found);
  if(found != std::string::npos) {
    role_start = found + 5;
    ++found;
    found = slave_text.find("\r\n",found);
    if(found != std::string::npos) {
      role_end = found - 1;
      auto role_str = slave_text.substr(role_start, role_end-role_start+1);
      std::cout << role_str << std::endl;
    }
  }

  found = 0;
  ++found;
  found = slave_text.find("master_host",found);
  if(found != std::string::npos) {
    ip_start = found + 12;
    ++found;
    found = slave_text.find("master_port",found);
    if(found != std::string::npos) {
      ip_end = found - 3;
      port_start = found + 12;
      ++found;
      found = slave_text.find("master_link_status",found);
      if(found != std::string::npos) {
        port_end = found - 3;
        auto &&ip = slave_text.substr(ip_start, ip_end-ip_start+1);
        auto &&port = static_cast<uint16_t>(std::stoi(slave_text.substr(port_start, port_end-port_start+1)));
        master_ip_port_pair = std::make_pair(ip, port);
      }
    }
  }
  std::cout << master_ip_port_pair.first << " " << master_ip_port_pair.second << std::endl;

  found = 0;
  while(found != std::string::npos){
    ++found;
    found = text.find("slave",found);
    if(found != std::string::npos) {
      ++found;
      found = text.find("ip",found);
      if(found != std::string::npos) {
        ip_start = found+3;
        ++found;
        found = text.find("port",found);
        if(found != std::string::npos) {
          ip_end = found-2;
          port_start = found+5;
          ++found;
          found = text.find("state",found);
          if(found != std::string::npos) {
            port_end = found-2;
            auto &&ip = text.substr(ip_start, ip_end-ip_start+1);
            int &&tmp_port = 6379;
            std::from_chars(text.data()+port_start, text.data()+port_end+1, tmp_port);
            auto &&port = static_cast<uint16_t>(tmp_port);
            auto &&pair = std::make_pair(ip, port);
            slave_ip_port_pair.emplace_back(pair);
          }
        }
      }
    }
  }
  for (auto pair:slave_ip_port_pair)
  {
    std::cout << pair.first << " " << pair.second << std::endl;
  }
  
  return 0;
}