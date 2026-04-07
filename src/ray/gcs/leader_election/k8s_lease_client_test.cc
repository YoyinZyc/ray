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

#include "ray/gcs/leader_election/k8s_lease_client.h"
#include "gtest/gtest.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"

namespace ray {
namespace gcs {

class K8sLeaseClientTest : public ::testing::Test {
 protected:
  std::string lease_namespace_ = "ray-cluster";
  std::string lease_key_ = "gcs-lease";
  std::string my_id_ = "node-1";
};

TEST_F(K8sLeaseClientTest, InitialAcquireSuccess) {
  auto get_api = [](const std::string &, nlohmann::json &) {
    return false;  // Lease does not exist initially
  };

  auto post_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return true;  // Creation succeeds
  };

  auto put_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return false;
  };

  K8sLeaseClient client(lease_namespace_, get_api, post_api, put_api);
  EXPECT_TRUE(client.TryAcquire(lease_key_, my_id_, 10000));
}

TEST_F(K8sLeaseClientTest, AcquireFailsWhenHeldActive) {
  auto get_api = [&](const std::string &, nlohmann::json &resp) {
    // Held by another ID, not expired
    resp["spec"]["holderIdentity"] = "node-2";
    resp["spec"]["leaseDurationSeconds"] = 10;
    absl::Time future = absl::Now() + absl::Seconds(100);
    resp["spec"]["renewTime"] = absl::FormatTime("%Y-%m-%dT%H:%M:%E6SZ", future, absl::UTCTimeZone());
    return true;
  };

  auto post_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return false;
  };

  auto put_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return false;
  };

  K8sLeaseClient client(lease_namespace_, get_api, post_api, put_api);
  EXPECT_FALSE(client.TryAcquire(lease_key_, my_id_, 10000));
}

TEST_F(K8sLeaseClientTest, AcquireSuccessWhenExpired) {
  auto get_api = [&](const std::string &, nlohmann::json &resp) {
    // Held by another ID, but expired
    resp["spec"]["holderIdentity"] = "node-2";
    resp["spec"]["leaseDurationSeconds"] = 1;
    absl::Time past = absl::Now() - absl::Seconds(100);
    resp["spec"]["renewTime"] = absl::FormatTime("%Y-%m-%dT%H:%M:%E6SZ", past, absl::UTCTimeZone());
    return true;
  };

  auto post_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return false;
  };

  auto put_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return true;  // Takeover PUT succeeds
  };

  K8sLeaseClient client(lease_namespace_, get_api, post_api, put_api);
  EXPECT_TRUE(client.TryAcquire(lease_key_, my_id_, 10000));
}

TEST_F(K8sLeaseClientTest, RenewSuccess) {
  auto get_api = [&](const std::string &, nlohmann::json &resp) {
    // Currently held by us
    resp["spec"]["holderIdentity"] = my_id_;
    resp["spec"]["leaseDurationSeconds"] = 10;
    resp["spec"]["renewTime"] = absl::FormatTime("%Y-%m-%dT%H:%M:%E6SZ", absl::Now(), absl::UTCTimeZone());
    return true;
  };

  auto post_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return false;
  };

  auto put_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return true;  // Renewal PUT succeeds
  };

  K8sLeaseClient client(lease_namespace_, get_api, post_api, put_api);
  EXPECT_TRUE(client.Renew(lease_key_, my_id_, 10000));
}

}  // namespace gcs
}  // namespace ray
