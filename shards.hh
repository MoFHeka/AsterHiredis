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
#pragma once

#include <memory>
#include <random>
#include <string>
#include <unordered_map>

#include "connection.hh"

namespace AsterHiredis {

using Slot = std::size_t;
static const std::size_t CLUSTER_SLOTS = 16383;

struct SlotRange {
  Slot min;
  Slot max;
};

inline bool operator<(const SlotRange &lhs, const SlotRange &rhs) {
  return lhs.max < rhs.max;
}

struct NodeHash {
  std::size_t operator()(const Node &node) const noexcept {
    auto host_hash = std::hash<std::string>{}(node.first);
    auto port_hash = std::hash<uint16_t>{}(node.second);
    return host_hash ^ (port_hash << 1);
  }
};

using Shards = std::map<SlotRange, Node>;
using ConnectionSPtr = ConnectionUnit::ConnectionSPtr;
using NodeMap = std::unordered_map<Node, ConnectionSPtr, NodeHash>;

class RedirectionError : public ConnectionUnit::ConnectionException {
 public:
  RedirectionError(const std::string message,
                   std::any incomplete_return = std::any{},
                   bool network_stack_error = true);

  RedirectionError(const RedirectionError &) = default;
  RedirectionError &operator=(const RedirectionError &) = default;

  RedirectionError(RedirectionError &&) = default;
  RedirectionError &operator=(RedirectionError &&) = default;

  virtual ~RedirectionError() = default;

  Slot slot() const { return slot_; }

  const Node &node() const { return node_; }

 private:
  std::pair<Slot, Node> _parse_error(const std::string &msg) const;

  Slot slot_ = 0;
  Node node_;
};

class MovedError : public RedirectionError {
 public:
  explicit MovedError(const std::string message,
                      std::any incomplete_return = std::any{},
                      bool network_stack_error = true)
    : RedirectionError(message, incomplete_return, network_stack_error){};

  MovedError(const MovedError &) = default;
  MovedError &operator=(const MovedError &) = default;

  MovedError(MovedError &&) = default;
  MovedError &operator=(MovedError &&) = default;

  virtual ~MovedError() = default;
};

class AskError : public RedirectionError {
 public:
  explicit AskError(const std::string message,
                    std::any incomplete_return = std::any{},
                    bool network_stack_error = true)
    : RedirectionError(message, incomplete_return, network_stack_error){};

  AskError(const AskError &) = default;
  AskError &operator=(const AskError &) = default;

  AskError(AskError &&) = default;
  AskError &operator=(AskError &&) = default;

  virtual ~AskError() = default;
};

namespace shards {
future<Shards> GetClusterSlots(const ConnectionSPtr connection);

future<ConnectionUnit::RedisReplyVectorSPtr>
ClusterSlotsCommand(const ConnectionSPtr connection);

Shards ParseReplyGetSlotInfo(const redisReply *const reply);

std::pair<SlotRange, Node> ParseSlotInfo(const redisReply *const reply);

// Get slot by key.
std::size_t GetSlot(const std::string_view &key);

// Randomly pick a slot.
std::size_t GetSlot();

Node GetNode(const Slot &slot, const Shards &shards);
}  // namespace shards

}  // namespace AsterHiredis