// Copyright 2017 The Ray Authors.
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

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_server.h"
#include "ray/gcs/leader_election/leader_election_client_interface.h"
#include "ray/gcs/metrics.h"
#include "ray/gcs/tests/gcs_server_test_helpers.h"
#include "ray/gcs_rpc_client/rpc_client.h"
#include "ray/observability/fake_metric.h"
#include "src/proto/grpc/health/v1/health.grpc.pb.h"

namespace ray {

// Verifies that a passive GCS server boots up and responds successfully
// to gRPC health checks with SERVING status even before being promoted to leader.
TEST_F(GcsServerTest, TestPassiveServerReadiness) {
  gcs::GcsServerConfig passive_config;
  passive_config.grpc_server_port = 0;
  passive_config.grpc_server_name = "PassiveGcsServer";
  passive_config.grpc_server_thread_num = 1;
  passive_config.redis_address = "127.0.0.1";
  passive_config.node_ip_address = "127.0.0.1";
  passive_config.enable_sharding_conn = false;
  passive_config.redis_port = TEST_REDIS_SERVER_PORTS.front();
  // Enable leader election to boot in passive.
  passive_config.ray_leader_elect_enabled = true;

  DedicatedGcsServerRunner passive_runner(passive_config, fake_metrics_);
  passive_runner.Start();
  int port = passive_runner.GetPort();

  // Create health check stub pointing to the passive GCS server.
  auto channel = grpc::CreateChannel("localhost:" + std::to_string(port),
                                     grpc::InsecureChannelCredentials());
  auto passive_health_check_stub = grpc::health::v1::Health::NewStub(channel);

  // Check health of the passive server.
  grpc::health::v1::HealthCheckRequest request;
  grpc::health::v1::HealthCheckResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto status = passive_health_check_stub->Check(&context, request, &response);

  ASSERT_TRUE(status.ok());
  EXPECT_EQ(response.status(), grpc::health::v1::HealthCheckResponse::SERVING);

  passive_runner.Stop();
}

// Verifies that a passive GCS server blocks mutating RPC requests (returning
// GcsPassive status), but successfully processes loaded table data and allows
// mutating requests once it is promoted to active leader.
TEST_F(GcsServerTest, TestPassivePromotion) {
  // 1. Pre-populate a node in Redis using the active GCS server/client.
  auto gcs_node_info = GenNodeInfo(9, "127.0.0.9", "promoted_node");
  rpc::RegisterNodeRequest register_node_info_request;
  register_node_info_request.mutable_node_info()->CopyFrom(*gcs_node_info);
  ASSERT_TRUE(RegisterNode(register_node_info_request));

  gcs::GcsServerConfig passive_config;
  passive_config.grpc_server_port = 0;
  passive_config.grpc_server_name = "PassiveGcsServerForPromotion";
  passive_config.grpc_server_thread_num = 1;
  passive_config.redis_address = "127.0.0.1";
  passive_config.node_ip_address = "127.0.0.1";
  passive_config.enable_sharding_conn = false;
  passive_config.redis_port = TEST_REDIS_SERVER_PORTS.front();
  // Enable leader election to boot in passive.
  passive_config.ray_leader_elect_enabled = true;

  DedicatedGcsServerRunner passive_runner(passive_config, fake_metrics_);
  passive_runner.Start();
  int port = passive_runner.GetPort();

  // Create client pointing to the passive GCS server.
  auto passive_client =
      std::make_unique<rpc::GcsRpcClient>("0.0.0.0", port, *client_call_manager_);

  // Verify it is responsive to healthcheck.
  auto channel = grpc::CreateChannel("localhost:" + std::to_string(port),
                                     grpc::InsecureChannelCredentials());
  auto passive_health_check_stub = grpc::health::v1::Health::NewStub(channel);
  grpc::health::v1::HealthCheckRequest request;
  grpc::health::v1::HealthCheckResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto status = passive_health_check_stub->Check(&context, request, &response);
  ASSERT_TRUE(status.ok());

  // 2. Before promotion, verify that the passive GCS has an empty node cache.
  {
    std::vector<rpc::GcsNodeInfo> nodes;
    std::promise<bool> promise;
    rpc::GetAllNodeInfoRequest get_all_request;
    passive_client->GetAllNodeInfo(
        std::move(get_all_request),
        [&nodes, &promise](const Status &status, const rpc::GetAllNodeInfoReply &reply) {
          RAY_CHECK_OK(status);
          for (int index = 0; index < reply.node_info_list_size(); ++index) {
            nodes.push_back(reply.node_info_list(index));
          }
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
    // It must return 0 nodes because GCS has not been promoted and has not loaded Redis.
    ASSERT_EQ(nodes.size(), 0);
  }

  // Before promotion, verify that mutating requests are blocked on passive GCS server!
  {
    std::promise<bool> promise;
    rpc::RegisterNodeRequest register_request;
    register_request.mutable_node_info()->CopyFrom(
        *GenNodeInfo(10, "127.0.0.10", "passive_node"));
    passive_client->RegisterNode(
        std::move(register_request),
        [&promise](const Status &status, const rpc::RegisterNodeReply &reply) {
          EXPECT_TRUE(status.IsGcsPassive());
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
  }

  // 3. Trigger promotion
  passive_runner.GetServer().TriggerPromotion();

  // Wait for the async load from Redis to finish and the server to become leader.
  ASSERT_TRUE(WaitForCondition([&]() { return passive_runner.GetServer().IsLeader(); },
                               /*timeout_ms=*/5000));

  // 4. After promotion, verify that the pre-populated node is now loaded inside GCS
  // cache!
  {
    std::vector<rpc::GcsNodeInfo> nodes;
    std::promise<bool> promise;
    rpc::GetAllNodeInfoRequest get_all_request;
    passive_client->GetAllNodeInfo(
        std::move(get_all_request),
        [&nodes, &promise](const Status &status, const rpc::GetAllNodeInfoReply &reply) {
          RAY_CHECK_OK(status);
          for (int index = 0; index < reply.node_info_list_size(); ++index) {
            nodes.push_back(reply.node_info_list(index));
          }
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
    // The pre-populated node must be successfully resolved and returned!
    ASSERT_GE(nodes.size(), 1);
    bool found_node = false;
    for (const auto &node : nodes) {
      if (node.node_id() == gcs_node_info->node_id()) {
        found_node = true;
        EXPECT_EQ(node.state(), rpc::GcsNodeInfo::ALIVE);
        EXPECT_EQ(node.node_manager_address(), "127.0.0.9");
      }
    }
    ASSERT_TRUE(found_node);
  }

  // 5. After promotion, verify that mutating requests can now be handled successfully!
  {
    std::promise<bool> promise;
    rpc::RegisterNodeRequest register_request;
    register_request.mutable_node_info()->CopyFrom(
        *GenNodeInfo(10, "127.0.0.10", "passive_node"));
    passive_client->RegisterNode(
        std::move(register_request),
        [&promise](const Status &status, const rpc::RegisterNodeReply &reply) {
          EXPECT_TRUE(status.ok());
          EXPECT_EQ(reply.status().code(), static_cast<int>(StatusCode::OK));
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
  }

  passive_runner.Stop();
}

// Verifies that local head node registration requests are allowed to bypass passive
// GCS gating. On promotion, the cached registration is properly published and old head
// nodes are marked dead in Redis.
TEST_F(GcsServerTest, TestPassiveHeadNodeRegistrationAndPromotion) {
  // 1. Pre-populate stale head node A in Redis using active GCS server.
  auto stale_head_info = GenNodeInfo(9, "127.0.0.9", "stale_head_node");
  stale_head_info->set_is_head_node(true);
  rpc::RegisterNodeRequest register_node_info_request;
  register_node_info_request.mutable_node_info()->CopyFrom(*stale_head_info);
  ASSERT_TRUE(RegisterNode(register_node_info_request));

  gcs::GcsServerConfig passive_config;
  passive_config.grpc_server_port = 0;
  passive_config.grpc_server_name = "PassiveGcsServerForHeadReg";
  passive_config.grpc_server_thread_num = 1;
  passive_config.redis_address = "127.0.0.1";
  passive_config.node_ip_address = "127.0.0.1";
  passive_config.enable_sharding_conn = false;
  passive_config.redis_port = TEST_REDIS_SERVER_PORTS.front();
  passive_config.ray_leader_elect_enabled = true;

  DedicatedGcsServerRunner passive_runner(passive_config, fake_metrics_);
  passive_runner.Start();
  int port = passive_runner.GetPort();

  // Create client pointing to passive GCS server.
  auto passive_client =
      std::make_unique<rpc::GcsRpcClient>("0.0.0.0", port, *client_call_manager_);

  // 2. Register new head node B (ALIVE) on passive GCS server.
  // It must bypass passive gating and return Status::OK!
  auto new_head_info = GenNodeInfo(10, "127.0.0.1", "new_head_node");
  new_head_info->set_is_head_node(true);
  NodeID new_node_id = NodeID::FromBinary(new_head_info->node_id());
  {
    std::promise<bool> promise;
    rpc::RegisterNodeRequest register_request;
    register_request.mutable_node_info()->CopyFrom(*new_head_info);
    passive_client->RegisterNode(
        std::move(register_request),
        [&promise](const Status &status, const rpc::RegisterNodeReply &reply) {
          EXPECT_TRUE(status.ok());
          EXPECT_EQ(reply.status().code(), static_cast<int>(StatusCode::OK));
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
  }

  // Verify B is NOT in Redis yet (still cached in passive).
  {
    std::promise<bool> promise;
    rpc::GetAllNodeInfoRequest get_all_request;
    client_->GetAllNodeInfo(  // Queries active Redis
        std::move(get_all_request),
        [new_node_id, &promise](const Status &status,
                                const rpc::GetAllNodeInfoReply &reply) {
          RAY_CHECK_OK(status);
          bool found_new_head = false;
          for (int index = 0; index < reply.node_info_list_size(); ++index) {
            if (reply.node_info_list(index).node_id() == new_node_id.Binary()) {
              found_new_head = true;
            }
          }
          EXPECT_FALSE(found_new_head);
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
  }

  // 3. Trigger GCS promotion on passive server.
  passive_runner.GetServer().TriggerPromotion();

  // 4. After promotion, verify that stale head node A is marked DEAD, and new head node B
  // is registered ALIVE in Redis. Promotion flushes the cached head-node registration to
  // Redis asynchronously (async table load followed by PromoteNodeManager delegation), so
  // poll GetAllNodeInfo until the expected end state is observed instead of relying on a
  // fixed delay.
  {
    ASSERT_TRUE(WaitForCondition(
        [&]() {
          std::promise<bool> promise;
          bool head_promoted = false;
          rpc::GetAllNodeInfoRequest get_all_request;
          passive_client->GetAllNodeInfo(
              std::move(get_all_request),
              [stale_head_info, new_node_id, &promise, &head_promoted](
                  const Status &status, const rpc::GetAllNodeInfoReply &reply) {
                RAY_CHECK_OK(status);
                bool found_stale_head_dead = false;
                bool found_new_head_alive = false;
                for (int index = 0; index < reply.node_info_list_size(); ++index) {
                  const auto &node = reply.node_info_list(index);
                  if (node.node_id() == stale_head_info->node_id() &&
                      node.state() == rpc::GcsNodeInfo::DEAD) {
                    found_stale_head_dead = true;
                  }
                  if (node.node_id() == new_node_id.Binary() &&
                      node.state() == rpc::GcsNodeInfo::ALIVE) {
                    found_new_head_alive = true;
                  }
                }
                head_promoted = found_stale_head_dead && found_new_head_alive;
                promise.set_value(true);
              });
          EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
          return head_promoted;
        },
        /*timeout_ms=*/5000));
  }

  passive_runner.Stop();
}

// Verifies end-to-end lease acquisition and leader promotion where a passive GCS
// elector periodically attempts to acquire the lease, promoting to leader and allowing
// mutating RPCs once granted.
TEST_F(GcsServerTest, TestLeaderElectionE2EPromotion) {
  gcs::GcsServerConfig passive_config;
  passive_config.grpc_server_port = 0;
  passive_config.grpc_server_name = "ElectionGcsServer";
  passive_config.grpc_server_thread_num = 1;
  passive_config.redis_address = "127.0.0.1";
  passive_config.node_ip_address = "127.0.0.1";
  passive_config.enable_sharding_conn = false;
  passive_config.redis_port = TEST_REDIS_SERVER_PORTS.front();
  passive_config.ray_leader_elect_enabled = true;

  // Set tight elector loop timings for fast test execution (4s / 2s / 1s)
  passive_config.ray_leader_elect_lease_duration_seconds = 4;
  passive_config.ray_leader_elect_renew_deadline_seconds = 2;
  passive_config.ray_leader_elect_retry_period_seconds = 1;

  auto mock_client = std::make_shared<MockLeaderLeaseClient>();
  passive_config.mock_lease_client = mock_client;

  DedicatedGcsServerRunner passive_runner(passive_config, fake_metrics_);
  passive_runner.Start();

  // Initially, GCS is in passive mode (not leader).
  EXPECT_FALSE(passive_runner.GetServer().IsLeader());

  int port = passive_runner.GetPort();
  auto passive_client =
      std::make_unique<rpc::GcsRpcClient>("0.0.0.0", port, *client_call_manager_);

  // Mutating requests should be blocked.
  {
    std::promise<bool> promise;
    rpc::RegisterNodeRequest register_request;
    register_request.mutable_node_info()->CopyFrom(
        *GenNodeInfo(10, "127.0.0.10", "passive_node"));
    passive_client->RegisterNode(
        std::move(register_request),
        [&promise](const Status &status, const rpc::RegisterNodeReply &reply) {
          EXPECT_TRUE(status.IsGcsPassive());
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
  }

  // Grant lease to the passive server.
  mock_client->SetGrantLease(true);

  // Wait for the background loop to acquire lease and promote GCS.
  // It should happen within 1 second.
  bool promoted = false;
  for (int i = 0; i < 20; ++i) {
    if (passive_runner.GetServer().IsLeader()) {
      promoted = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_TRUE(promoted);

  // After promotion, mutating requests must succeed.
  {
    std::promise<bool> promise;
    rpc::RegisterNodeRequest register_request;
    register_request.mutable_node_info()->CopyFrom(
        *GenNodeInfo(10, "127.0.0.10", "passive_node"));
    passive_client->RegisterNode(
        std::move(register_request),
        [&promise](const Status &status, const rpc::RegisterNodeReply &reply) {
          EXPECT_TRUE(status.ok());
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
  }

  passive_runner.Stop();
}

// Verifies that promotion is idempotent: invoking the deferred promotion multiple times
// (as can happen if the leader election callback fires more than once) loads the tables
// and registers the pre-populated node exactly once, without crashing or duplicating.
TEST_F(GcsServerTest, TestPromotionIsIdempotent) {
  // Pre-populate a node in Redis via the active GCS server.
  auto gcs_node_info = GenNodeInfo(9, "127.0.0.9", "idempotent_node");
  rpc::RegisterNodeRequest register_node_info_request;
  register_node_info_request.mutable_node_info()->CopyFrom(*gcs_node_info);
  ASSERT_TRUE(RegisterNode(register_node_info_request));

  gcs::GcsServerConfig passive_config;
  passive_config.grpc_server_port = 0;
  passive_config.grpc_server_name = "PassiveGcsServerIdempotent";
  passive_config.grpc_server_thread_num = 1;
  passive_config.redis_address = "127.0.0.1";
  passive_config.node_ip_address = "127.0.0.1";
  passive_config.enable_sharding_conn = false;
  passive_config.redis_port = TEST_REDIS_SERVER_PORTS.front();
  passive_config.ray_leader_elect_enabled = true;

  DedicatedGcsServerRunner passive_runner(passive_config, fake_metrics_);
  passive_runner.Start();
  int port = passive_runner.GetPort();

  auto passive_client =
      std::make_unique<rpc::GcsRpcClient>("0.0.0.0", port, *client_call_manager_);

  // Trigger promotion multiple times. The promotion_started_ guard must ensure the
  // deferred load only runs once.
  passive_runner.GetServer().TriggerPromotion();
  passive_runner.GetServer().TriggerPromotion();
  passive_runner.GetServer().TriggerPromotion();

  // Wait for the async load to finish.
  ASSERT_TRUE(WaitForCondition([&]() { return passive_runner.GetServer().IsLeader(); },
                               /*timeout_ms=*/5000));

  // The pre-populated node must appear exactly once (not duplicated by multiple loads).
  {
    std::vector<rpc::GcsNodeInfo> nodes;
    std::promise<bool> promise;
    rpc::GetAllNodeInfoRequest get_all_request;
    passive_client->GetAllNodeInfo(
        std::move(get_all_request),
        [&nodes, &promise](const Status &status, const rpc::GetAllNodeInfoReply &reply) {
          RAY_CHECK_OK(status);
          for (int index = 0; index < reply.node_info_list_size(); ++index) {
            nodes.push_back(reply.node_info_list(index));
          }
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
    int count = 0;
    for (const auto &node : nodes) {
      if (node.node_id() == gcs_node_info->node_id()) {
        count++;
      }
    }
    ASSERT_EQ(count, 1);
  }

  EXPECT_TRUE(passive_runner.GetServer().IsLeader());
  passive_runner.Stop();
}

// Verifies that a graceful shutdown of a leader GCS voluntarily releases the lease,
// allowing a passive server to take over immediately.
TEST_F(GcsServerTest, TestGracefulShutdownReleasesLease) {
  gcs::GcsServerConfig passive_config;
  passive_config.grpc_server_port = 0;
  passive_config.grpc_server_name = "ReleaseLeaseGcsServer";
  passive_config.grpc_server_thread_num = 1;
  passive_config.redis_address = "127.0.0.1";
  passive_config.node_ip_address = "127.0.0.1";
  passive_config.enable_sharding_conn = false;
  passive_config.redis_port = TEST_REDIS_SERVER_PORTS.front();
  passive_config.ray_leader_elect_enabled = true;
  passive_config.ray_leader_elect_lease_duration_seconds = 4;
  passive_config.ray_leader_elect_renew_deadline_seconds = 2;
  passive_config.ray_leader_elect_retry_period_seconds = 1;

  auto mock_client = std::make_shared<MockLeaderLeaseClient>();
  mock_client->SetGrantLease(true);
  passive_config.mock_lease_client = mock_client;

  DedicatedGcsServerRunner passive_runner(passive_config, fake_metrics_);
  passive_runner.Start();

  // Wait for the server to acquire leadership.
  bool promoted = false;
  for (int i = 0; i < 20; ++i) {
    if (passive_runner.GetServer().IsLeader()) {
      promoted = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_TRUE(promoted);

  // Graceful shutdown must voluntarily release the lease exactly once.
  passive_runner.Stop();
  EXPECT_EQ(mock_client->GetReleaseCount(), 1);
}

// Verifies that when leader election is disabled (legacy single-head cluster), the GCS
// becomes leader immediately, never constructs an elector, and serves mutating RPCs
// without any promotion step.
TEST_F(GcsServerTest, TestLegacySingleHeadUnaffected) {
  // The default fixture server (gcs_server_) is started with leader election disabled.
  // It must already be the active leader.
  EXPECT_TRUE(gcs_server_->IsLeader());

  // A full register-node round-trip must succeed immediately (no promotion needed).
  auto gcs_node_info = GenNodeInfo(1, "127.0.0.1", "legacy_node");
  rpc::RegisterNodeRequest register_node_info_request;
  register_node_info_request.mutable_node_info()->CopyFrom(*gcs_node_info);
  ASSERT_TRUE(RegisterNode(register_node_info_request));

  std::vector<rpc::GcsNodeInfo> node_info_list = GetAllNodeInfo();
  ASSERT_EQ(node_info_list.size(), 1);
  EXPECT_EQ(node_info_list[0].state(), rpc::GcsNodeInfo::ALIVE);

  // Health check reports SERVING for the active single head.
  EXPECT_TRUE(WaitForHealthStatus(grpc::health::v1::HealthCheckResponse::SERVING,
                                  std::chrono::seconds(10)));
}

// Verifies that when two GCS servers both boot in passive mode and contend for the same
// lease, exactly one is elected leader (mutual exclusion). The winner promotes and
// accepts mutating RPCs; the loser stays passive and rejects them.
TEST_F(GcsServerTest, TestTwoPassiveServersElectOneLeader) {
  // A single shared lease client that both servers contend on (the analog of a shared
  // Redis/Kubernetes lease). It grants the lease to whichever candidate asks first.
  auto shared_client = std::make_shared<MockLeaderLeaseClient>();
  shared_client->SetGrantLease(true);

  auto make_config = [&](const std::string &name) {
    gcs::GcsServerConfig config;
    config.grpc_server_port = 0;
    config.grpc_server_name = name;
    config.grpc_server_thread_num = 1;
    config.redis_address = "127.0.0.1";
    config.node_ip_address = "127.0.0.1";
    config.enable_sharding_conn = false;
    config.redis_port = TEST_REDIS_SERVER_PORTS.front();
    config.ray_leader_elect_enabled = true;
    config.ray_leader_elect_lease_duration_seconds = 4;
    config.ray_leader_elect_renew_deadline_seconds = 2;
    config.ray_leader_elect_retry_period_seconds = 1;
    config.mock_lease_client = shared_client;
    // Distinct node IDs so the two candidates get distinct lease holder ids. Both run in
    // the same test process, so without this they would fall back to the same
    // hostname:pid holder id and the lease could not distinguish them.
    config.node_id = NodeID::FromRandom().Hex();
    return config;
  };

  DedicatedGcsServerRunner runner_a(make_config("ContendingGcsServerA"), fake_metrics_);
  DedicatedGcsServerRunner runner_b(make_config("ContendingGcsServerB"), fake_metrics_);
  runner_a.Start();
  runner_b.Start();

  // Wait until exactly one of them has been promoted to leader.
  bool one_elected = false;
  for (int i = 0; i < 30; ++i) {
    if (runner_a.GetServer().IsLeader() != runner_b.GetServer().IsLeader()) {
      one_elected = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_TRUE(one_elected);
  // Mutual exclusion: never both leaders.
  ASSERT_FALSE(runner_a.GetServer().IsLeader() && runner_b.GetServer().IsLeader());

  DedicatedGcsServerRunner &leader =
      runner_a.GetServer().IsLeader() ? runner_a : runner_b;
  DedicatedGcsServerRunner &passive =
      runner_a.GetServer().IsLeader() ? runner_b : runner_a;

  // The leader accepts mutating RPCs; the passive rejects them (it never won the lease).
  // Use dedicated clients scoped to this block so they (and their in-flight calls) are
  // fully torn down before the servers stop, avoiding a gRPC shutdown race.
  {
    auto leader_client = std::make_unique<rpc::GcsRpcClient>(
        "0.0.0.0", leader.GetPort(), *client_call_manager_);
    auto passive_client = std::make_unique<rpc::GcsRpcClient>(
        "0.0.0.0", passive.GetPort(), *client_call_manager_);

    // The leader accepts mutating RPCs.
    {
      std::promise<bool> promise;
      rpc::RegisterNodeRequest register_request;
      register_request.mutable_node_info()->CopyFrom(
          *GenNodeInfo(11, "127.0.0.11", "leader_node"));
      leader_client->RegisterNode(
          std::move(register_request),
          [&promise](const Status &status, const rpc::RegisterNodeReply &reply) {
            EXPECT_TRUE(status.ok());
            promise.set_value(true);
          });
      EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
    }

    // The passive still rejects mutating RPCs (it never won the lease).
    {
      std::promise<bool> promise;
      rpc::RegisterNodeRequest register_request;
      register_request.mutable_node_info()->CopyFrom(
          *GenNodeInfo(12, "127.0.0.12", "passive_node"));
      passive_client->RegisterNode(
          std::move(register_request),
          [&promise](const Status &status, const rpc::RegisterNodeReply &reply) {
            EXPECT_TRUE(status.IsGcsPassive());
            promise.set_value(true);
          });
      EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
    }
  }

  runner_a.Stop();
  runner_b.Stop();
}

// Verifies failover: two GCS servers contend for a shared lease, one is elected leader,
// then the leader is stopped (graceful shutdown releases the lease). The passive's
// elector must then acquire the now-vacant lease, promote itself, and start accepting
// mutating RPCs.
TEST_F(GcsServerTest, TestTwoPassiveServersFailover) {
  auto shared_client = std::make_shared<MockLeaderLeaseClient>();
  shared_client->SetGrantLease(true);

  auto make_config = [&](const std::string &name) {
    gcs::GcsServerConfig config;
    config.grpc_server_port = 0;
    config.grpc_server_name = name;
    config.grpc_server_thread_num = 1;
    config.redis_address = "127.0.0.1";
    config.node_ip_address = "127.0.0.1";
    config.enable_sharding_conn = false;
    config.redis_port = TEST_REDIS_SERVER_PORTS.front();
    config.ray_leader_elect_enabled = true;
    // Tight timings so the passive takes over quickly after the leader releases.
    config.ray_leader_elect_lease_duration_seconds = 4;
    config.ray_leader_elect_renew_deadline_seconds = 2;
    config.ray_leader_elect_retry_period_seconds = 1;
    config.mock_lease_client = shared_client;
    // Distinct node IDs -> distinct lease holder ids (both run in the same process).
    config.node_id = NodeID::FromRandom().Hex();
    return config;
  };

  auto runner_a = std::make_unique<DedicatedGcsServerRunner>(
      make_config("FailoverGcsServerA"), fake_metrics_);
  auto runner_b = std::make_unique<DedicatedGcsServerRunner>(
      make_config("FailoverGcsServerB"), fake_metrics_);
  runner_a->Start();
  runner_b->Start();

  // Wait until exactly one is elected leader.
  bool one_elected = false;
  for (int i = 0; i < 30; ++i) {
    if (runner_a->GetServer().IsLeader() != runner_b->GetServer().IsLeader()) {
      one_elected = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_TRUE(one_elected);

  const bool a_is_leader = runner_a->GetServer().IsLeader();
  std::unique_ptr<DedicatedGcsServerRunner> &leader = a_is_leader ? runner_a : runner_b;
  std::unique_ptr<DedicatedGcsServerRunner> &passive = a_is_leader ? runner_b : runner_a;

  // The passive is still passive: it must reject mutating RPCs.
  {
    auto passive_client = std::make_unique<rpc::GcsRpcClient>(
        "0.0.0.0", passive->GetPort(), *client_call_manager_);
    std::promise<bool> promise;
    rpc::RegisterNodeRequest register_request;
    register_request.mutable_node_info()->CopyFrom(
        *GenNodeInfo(20, "127.0.0.20", "passive_before_failover"));
    passive_client->RegisterNode(
        std::move(register_request),
        [&promise](const Status &status, const rpc::RegisterNodeReply &reply) {
          EXPECT_TRUE(status.IsGcsPassive());
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
  }

  // Simulate leader failure: graceful stop releases the lease, freeing it for the
  // passive.
  leader->Stop();
  leader.reset();

  // The passive's elector should acquire the vacant lease and promote itself.
  bool passive_promoted = false;
  for (int i = 0; i < 50; ++i) {
    if (passive->GetServer().IsLeader()) {
      passive_promoted = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_TRUE(passive_promoted);

  // After promotion, the (former) passive must accept mutating RPCs.
  {
    auto promoted_client = std::make_unique<rpc::GcsRpcClient>(
        "0.0.0.0", passive->GetPort(), *client_call_manager_);
    std::promise<bool> promise;
    rpc::RegisterNodeRequest register_request;
    register_request.mutable_node_info()->CopyFrom(
        *GenNodeInfo(21, "127.0.0.21", "passive_after_failover"));
    promoted_client->RegisterNode(
        std::move(register_request),
        [&promise](const Status &status, const rpc::RegisterNodeReply &reply) {
          EXPECT_TRUE(status.ok());
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
  }

  passive->Stop();
}

}  // namespace ray
