// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <functional>
#include <memory>
#include <string>
#include "nlohmann/json.hpp"
#include "ray/gcs/leader_election/leader_election_client_interface.h"

namespace ray {
namespace gcs {

/// Concrete implementation of the LeaderLeaseClientInterface using Kubernetes Leases.
class K8sLeaseClient : public LeaderLeaseClientInterface {
 public:
  K8sLeaseClient(
      std::string lease_namespace,
      std::function<bool(const std::string &, nlohmann::json &)> get_api,
      std::function<bool(const std::string &, const nlohmann::json &, nlohmann::json &)>
          post_api,
      std::function<bool(const std::string &, const nlohmann::json &, nlohmann::json &)>
          put_api);

  bool TryAcquire(const std::string &lease_key,
                  const std::string &holder_id,
                  int ttl_ms) override;

  bool Renew(const std::string &lease_key,
             const std::string &holder_id,
             int ttl_ms) override;

  void Release(const std::string &lease_key,
               const std::string &holder_id) override;

 private:
  std::string lease_namespace_;
  std::function<bool(const std::string &, nlohmann::json &)> get_api_;
  std::function<bool(const std::string &, const nlohmann::json &, nlohmann::json &)>
      post_api_;
  std::function<bool(const std::string &, const nlohmann::json &, nlohmann::json &)>
      put_api_;
};

}  // namespace gcs
}  // namespace ray
