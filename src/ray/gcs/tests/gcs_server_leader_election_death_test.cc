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

// This death test lives in its own test binary on purpose. gtest death tests use fork(),
// which is unsafe in a heavily-threaded process. By keeping this the only test in the
// binary (and using a thread-light fixture that does NOT start a shared GCS server in
// SetUp), the process forks with a minimal, consistent thread state, so the forked child
// reliably reaches the watchdog's RAY_LOG(FATAL) step-down path instead of crashing
// early.

#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_server.h"
#include "ray/gcs/tests/gcs_server_test_helpers.h"

namespace ray {

class GcsServerDeathTest : public ::testing::Test {
 public:
  GcsServerDeathTest() {
    // A graceful Redis shutdown persists its dataset to dump.rdb (and appendonly.aof)
    // in the test's working directory. Remove them before/after the test so a freshly
    // started Redis does not load a previous run's dataset and leak stale GCS state.
    std::remove("dump.rdb");
    std::remove("appendonly.aof");
    TestSetupUtil::StartUpRedisServers(std::vector<int>());
  }
  ~GcsServerDeathTest() override {
    TestSetupUtil::ShutDownRedisServers();
    // See the constructor: clear persisted Redis files so the next test starts clean.
    std::remove("dump.rdb");
    std::remove("appendonly.aof");
  }

 protected:
  FakeGcsServerMetricsHolder metrics_holder_;
};

// Verifies that when GCS loses its leadership lease and fails to renew it,
// the background watchdog loop aborts the GCS process (fatal step-down)
// to prevent split-brain issues.
// NOTE: This test must live in its own isolated binary because gtest EXPECT_DEATH
// forks, which is unsafe/flaky in heavily-threaded processes.
TEST_F(GcsServerDeathTest, TestLeaderElectionStepDownDeathTest) {
  gcs::GcsServerConfig passive_config;
  passive_config.grpc_server_port = 0;
  passive_config.grpc_server_name = "DeathElectionGcsServer";
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
  mock_client->SetGrantLease(true);
  passive_config.mock_lease_client = mock_client;

  // Use EXPECT_DEATH to catch fate-sharing process suicide. The whole server lifecycle
  // runs inside the forked child: only that child's threads exist, so it reaches the
  // watchdog step-down cleanly.
  EXPECT_DEATH(
      {
        DedicatedGcsServerRunner passive_runner(passive_config,
                                                metrics_holder_.metrics());
        passive_runner.Start();

        // Wait for it to become leader.
        bool promoted = false;
        for (int i = 0; i < 20; ++i) {
          if (passive_runner.GetServer().IsLeader()) {
            promoted = true;
            break;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (!promoted) {
          std::exit(0);  // Exit peacefully to fail the death-test expectation.
        }

        // Simulate lease loss. The renewal loop observes it, and after the renew deadline
        // the watchdog thread calls RAY_LOG(FATAL), which aborts the process. Since this
        // runs in a forked child under EXPECT_DEATH, the process is killed the instant
        // FATAL fires, so a generous sleep only guards against slow CI scheduling and
        // does not slow down the passing case.
        mock_client->SetLoseLease(true);
        std::this_thread::sleep_for(std::chrono::seconds(15));
      },
      ".*Lost GCS leadership lease! Aborting to prevent split-brain.*");
}

}  // namespace ray
