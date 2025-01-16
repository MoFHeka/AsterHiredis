/*
 * Copyright (c) 2017 sewenew
 * Copyright (c) 2022, Jia He <mofhejia@163.com>
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "shards.hh"
#include "crc16.hpp"

namespace AsterHiredis {

RedirectionError::RedirectionError(const std::string message,
                                   std::any incomplete_return,
                                   bool network_stack_error)
  : ConnectionException(message, incomplete_return, network_stack_error) {
  std::tie(slot_, node_) = _parse_error(message_);
}

std::pair<Slot, Node>
RedirectionError::_parse_error(const std::string &msg) const {
  // "slot ip:port"
  auto space_pos = msg.find(" ");
  auto colon_pos = msg.find(":");
  if (space_pos == std::string::npos || colon_pos == std::string::npos ||
      colon_pos < space_pos) {
    throw ProtoError("[AsterHiredis] Invalid ASK/MOVED error message: " + msg);
  }

  try {
    auto slot = std::stoull(msg.substr(0, space_pos));
    auto host = msg.substr(space_pos + 1, colon_pos - space_pos - 1);
    auto port = static_cast<uint16_t>(std::stoi(msg.substr(colon_pos + 1)));

    return {slot, {host, port}};
  } catch (const std::exception &e) {
    throw ProtoError("[AsterHiredis] Invalid ASK/MOVED error message: " + msg);
  }
}

namespace shards {
future<Shards> GetClusterSlots(const ConnectionSPtr connection) {
  return do_with(std::move(connection), [](auto &connection) {
    return ClusterSlotsCommand(connection).then([](auto reply_vec_sptr) {
      auto reply_uptr = std::move(reply_vec_sptr->at(0));
      assert(reply_uptr.get());
      try {
        auto shards = ParseReplyGetSlotInfo(reply_uptr.get());
        return make_ready_future<Shards>(std::move(shards));
      } catch (const std::exception &e) {
        std::string err(e.what());
        return make_exception_future<Shards>(
          std::make_exception_ptr(ConnectionUnit::ConnectionException(err)));
      }
    });
  });
}

future<ConnectionUnit::RedisReplyVectorSPtr>
ClusterSlotsCommand(const ConnectionSPtr connection) {
  return connection->Send("CLUSTER SLOTS")
    .then([connection = std::move(connection)] { return connection->Recv(); });
}

Shards ParseReplyGetSlotInfo(const redisReply *const reply) {
  if (!reply::is_array(reply)) {
    throw ProtoError("[AsterHiredis] Expect ARRAY reply");
  }

  if (reply->element == nullptr || reply->elements == 0) {
    throw ProtoError("Empty slots");
  }

  Shards shards;
  for (std::size_t idx = 0; idx != reply->elements; ++idx) {
    auto *sub_reply = reply->element[idx];
    if (sub_reply == nullptr) {
      throw ProtoError("[AsterHiredis] Null slot info");
    }

    shards.emplace(ParseSlotInfo(sub_reply));
  }

  return shards;
}

std::pair<SlotRange, Node> ParseSlotInfo(const redisReply *const reply) {
  if (reply->elements < 3 || reply->element == nullptr) {
    throw ProtoError("[AsterHiredis] Invalid slot info");
  }

  // Min slot id
  auto *min_slot_reply = reply->element[0];
  if (min_slot_reply == nullptr) {
    throw ProtoError("[AsterHiredis] Invalid min slot");
  }
  std::size_t min_slot = reply::parse<long long>(min_slot_reply);

  // Max slot id
  auto *max_slot_reply = reply->element[1];
  if (max_slot_reply == nullptr) {
    throw ProtoError("[AsterHiredis] Invalid max slot");
  }
  std::size_t max_slot = reply::parse<long long>(max_slot_reply);

  if (min_slot > max_slot) {
    throw ProtoError("[AsterHiredis] Invalid slot range");
  }

  // Master node info
  auto *node_reply = reply->element[2];
  if (node_reply == nullptr || !reply::is_array(node_reply) ||
      node_reply->element == nullptr || node_reply->elements < 2) {
    throw ProtoError("[AsterHiredis] Invalid node info");
  }

  auto master_host = reply::parse<std::string>(node_reply->element[0]);
  uint16_t master_port =
    static_cast<uint16_t>(reply::parse<long long>(node_reply->element[1]));

  // By now, we ignore node id and other replicas' info.

  return {SlotRange{min_slot, max_slot}, Node{master_host, master_port}};
}

Slot GetSlot(const std::string_view &key) {
  // The following code is copied from: https://redis.io/topics/cluster-spec
  // And I did some minor changes.

  const auto *k = key.data();
  auto keylen = key.size();

  // start-end indexes of { and }.
  std::size_t s = 0;
  std::size_t e = 0;

  // Search the first occurrence of '{'.
  for (s = 0; s < keylen; s++)
    if (k[s] == '{') break;

  // No '{' ? Hash the whole key. This is the base case.
  if (s == keylen) return crc16(k, keylen) & CLUSTER_SLOTS;

  // '{' found? Check if we have the corresponding '}'.
  for (e = s + 1; e < keylen; e++)
    if (k[e] == '}') break;

  // No '}' or nothing between {} ? Hash the whole key.
  if (e == keylen || e == s + 1) return crc16(k, keylen) & CLUSTER_SLOTS;

  // If we are here there is both a { and a } on its right. Hash
  // what is in the middle between { and }.
  return crc16(k + s + 1, e - s - 1) & CLUSTER_SLOTS;
}

Slot GetSlot() {
  static thread_local std::default_random_engine engine;

  std::uniform_int_distribution<std::size_t> uniform_dist(0, CLUSTER_SLOTS);

  return uniform_dist(engine);
}

Node GetNode(const Slot &slot, const Shards &shards) {
  auto shards_iter = shards.lower_bound(SlotRange{slot, slot});
  if (shards_iter == shards.end() || slot < shards_iter->first.min) {
    throw ProtoError("Slot is out of range: " + std::to_string(slot));
  }

  return shards_iter->second;
}
}  // namespace shards

}  // namespace AsterHiredis