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
#include "ray/util/logging.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"

namespace ray {
namespace gcs {

K8sLeaseClient::K8sLeaseClient(
    std::string lease_namespace,
    std::function<bool(const std::string &, nlohmann::json &)> get_api,
    std::function<bool(const std::string &, const nlohmann::json &, nlohmann::json &)>
        post_api,
    std::function<bool(const std::string &, const nlohmann::json &, nlohmann::json &)>
        put_api)
    : lease_namespace_(std::move(lease_namespace)),
      get_api_(std::move(get_api)),
      post_api_(std::move(post_api)),
      put_api_(std::move(put_api)) {}

bool K8sLeaseClient::TryAcquire(const std::string &lease_key,
                                const std::string &holder_id,
                                int ttl_ms) {
  std::string get_path = "/apis/coordination.k8s.io/v1/namespaces/" + lease_namespace_ +
                         "/leases/" + lease_key;
  nlohmann::json response;
  bool exists = get_api_(get_path, response);

  absl::Time now = absl::Now();
  if (exists && response.contains("__api_server_date__")) {
    std::string date_str = response["__api_server_date__"].get<std::string>();
    std::string err;
    if (!absl::ParseTime(absl::RFC1123_full, date_str, &now, &err)) {
      RAY_LOG(WARNING) << "Failed to parse API server date: " << date_str << ", error: " << err;
    }
  }
  std::string now_str = absl::FormatTime("%Y-%m-%dT%H:%M:%E6SZ", now, absl::UTCTimeZone());
  int ttl_seconds = ttl_ms / 1000;

  if (!exists) {
    nlohmann::json create_req = {{"apiVersion", "coordination.k8s.io/v1"},
                                 {"kind", "Lease"},
                                 {"metadata",
                                  {{"name", lease_key},
                                   {"namespace", lease_namespace_}}},
                                 {"spec",
                                  {{"holderIdentity", holder_id},
                                   {"leaseDurationSeconds", ttl_seconds},
                                   {"renewTime", now_str}}}};

    std::string post_path = "/apis/coordination.k8s.io/v1/namespaces/" +
                            lease_namespace_ + "/leases";
    nlohmann::json create_resp;
    if (post_api_(post_path, create_req, create_resp)) {
      RAY_LOG(INFO) << "Successfully created Lease and acquired leadership.";
      return true;
    }
    return false;
  }

  std::string current_holder = "";
  if (response.contains("spec") && response["spec"].contains("holderIdentity")) {
    current_holder = response["spec"]["holderIdentity"].get<std::string>();
  }

  int current_duration = 10;
  if (response.contains("spec") && response["spec"].contains("leaseDurationSeconds")) {
    current_duration = response["spec"]["leaseDurationSeconds"].get<int>();
  }

  std::string renew_str = "";
  if (response.contains("spec") && response["spec"].contains("renewTime")) {
    renew_str = response["spec"]["renewTime"].get<std::string>();
  }

  std::string resource_version = "";
  if (response.contains("metadata") && response["metadata"].contains("resourceVersion")) {
    resource_version = response["metadata"]["resourceVersion"].get<std::string>();
  }

  absl::Time renew_time = absl::UnixEpoch();
  std::string parse_err;
  if (!absl::ParseTime(absl::RFC3339_full, renew_str, &renew_time, &parse_err)) {
    renew_time = now;
  }

  bool can_acquire = false;
  if (current_holder == holder_id) {
    can_acquire = true;
  } else {
    absl::Time expiration_time = renew_time + absl::Seconds(current_duration);
    if (now > expiration_time) {
      can_acquire = true;
    }
  }

  if (can_acquire) {
    nlohmann::json update_req = response;
    update_req["spec"]["holderIdentity"] = holder_id;
    update_req["spec"]["leaseDurationSeconds"] = ttl_seconds;
    update_req["spec"]["renewTime"] = now_str;

    if (!resource_version.empty()) {
      update_req["metadata"]["resourceVersion"] = resource_version;
    }

    std::string put_path = get_path;
    nlohmann::json update_resp;
    if (put_api_(put_path, update_req, update_resp)) {
      return true;
    }
  }

  return false;
}

bool K8sLeaseClient::Renew(const std::string &lease_key,
                           const std::string &holder_id,
                           int ttl_ms) {
  return TryAcquire(lease_key, holder_id, ttl_ms);
}

void K8sLeaseClient::Release(const std::string &lease_key,
                             const std::string &holder_id) {
  std::string get_path = "/apis/coordination.k8s.io/v1/namespaces/" + lease_namespace_ +
                         "/leases/" + lease_key;
  nlohmann::json response;
  if (!get_api_(get_path, response)) {
    return;
  }

  std::string current_holder = "";
  if (response.contains("spec") && response["spec"].contains("holderIdentity")) {
    current_holder = response["spec"]["holderIdentity"].get<std::string>();
  }

  if (current_holder == holder_id) {
    nlohmann::json update_req = response;
    update_req["spec"]["holderIdentity"] = "";
    update_req["spec"]["renewTime"] = "1970-01-01T00:00:00Z";

    std::string resource_version = "";
    if (response.contains("metadata") && response["metadata"].contains("resourceVersion")) {
      resource_version = response["metadata"]["resourceVersion"].get<std::string>();
    }
    if (!resource_version.empty()) {
      update_req["metadata"]["resourceVersion"] = resource_version;
    }

    nlohmann::json update_resp;
    put_api_(get_path, update_req, update_resp);
  }
}

}  // namespace gcs
}  // namespace ray
