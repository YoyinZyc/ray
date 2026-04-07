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

#include <string>

namespace ray {
namespace gcs {

/// Generic, platform-agnostic interface for distributed leader election and lease management.
class LeaderLeaseClientInterface {
 public:
  virtual ~LeaderLeaseClientInterface() = default;

  /// Attempt to acquire the distributed lock/lease.
  /// \param lease_key The unique name/identifier of the distributed lease.
  /// \param holder_id The identity of the participant attempting to acquire the lease.
  /// \param ttl_ms The requested lease duration in milliseconds.
  /// \return true if the lease was successfully acquired.
  virtual bool TryAcquire(const std::string &lease_key,
                          const std::string &holder_id,
                          int ttl_ms) = 0;

  /// Attempt to renew an existing lease.
  /// \param lease_key The unique name/identifier of the distributed lease.
  /// \param holder_id The identity of the participant attempting to renew the lease.
  /// \param ttl_ms The requested lease duration in milliseconds.
  /// \return true if the lease was successfully renewed.
  virtual bool Renew(const std::string &lease_key,
                     const std::string &holder_id,
                     int ttl_ms) = 0;

  /// Release the distributed lock/lease voluntarily.
  virtual void Release(const std::string &lease_key,
                       const std::string &holder_id) = 0;
};

}  // namespace gcs
}  // namespace ray
