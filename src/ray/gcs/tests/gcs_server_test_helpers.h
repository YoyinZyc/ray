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

#pragma once

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_server.h"
#include "ray/gcs/leader_election/leader_election_client_interface.h"
#include "ray/gcs/metrics.h"
#include "ray/gcs_rpc_client/rpc_client.h"
#include "ray/observability/fake_metric.h"
#include "src/proto/grpc/health/v1/health.grpc.pb.h"

namespace ray {

/// Bundles the full set of fake metrics required to construct a GcsServer in tests. Owns
/// the underlying fake metric objects and exposes an aggregate GcsServerMetrics view over
/// them, so tests don't have to repeat the ~20-field initializer boilerplate.
class FakeGcsServerMetricsHolder {
 public:
  FakeGcsServerMetricsHolder()
      : metrics_{
            actor_by_state_gauge_,
            gcs_actor_by_state_gauge_,
            running_job_gauge_,
            finished_job_counter_,
            job_duration_in_seconds_gauge_,
            placement_group_gauge_,
            placement_group_creation_latency_in_ms_histogram_,
            placement_group_scheduling_latency_in_ms_histogram_,
            placement_group_count_gauge_,
            task_events_reported_gauge_,
            task_events_dropped_gauge_,
            task_events_stored_gauge_,
            event_recorder_dropped_events_counter_,
            storage_operation_latency_in_ms_histogram_,
            storage_operation_count_counter_,
            resource_usage_gauge_,
            scheduler_placement_time_ms_histogram_,
            health_check_rpc_latency_ms_histogram_,
            io_context_monitor_latency_ms_gauge_,
            io_context_monitor_unhealthy_counter_,
        } {}

  gcs::GcsServerMetrics &metrics() { return metrics_; }

 private:
  observability::FakeGauge actor_by_state_gauge_;
  observability::FakeGauge gcs_actor_by_state_gauge_;
  observability::FakeGauge running_job_gauge_;
  observability::FakeCounter finished_job_counter_;
  observability::FakeGauge job_duration_in_seconds_gauge_;
  observability::FakeGauge placement_group_gauge_;
  observability::FakeHistogram placement_group_creation_latency_in_ms_histogram_;
  observability::FakeHistogram placement_group_scheduling_latency_in_ms_histogram_;
  observability::FakeGauge placement_group_count_gauge_;
  observability::FakeGauge task_events_reported_gauge_;
  observability::FakeGauge task_events_dropped_gauge_;
  observability::FakeGauge task_events_stored_gauge_;
  observability::FakeCounter event_recorder_dropped_events_counter_;
  observability::FakeHistogram storage_operation_latency_in_ms_histogram_;
  observability::FakeCounter storage_operation_count_counter_;
  observability::FakeGauge resource_usage_gauge_;
  observability::FakeHistogram scheduler_placement_time_ms_histogram_;
  observability::FakeHistogram health_check_rpc_latency_ms_histogram_;
  observability::FakeGauge io_context_monitor_latency_ms_gauge_;
  observability::FakeCounter io_context_monitor_unhealthy_counter_;
  gcs::GcsServerMetrics metrics_;
};

/// GcsServer subclass that exposes a hook to trigger the deferred promotion load directly
/// (bypassing the leader elector) for tests.
class GcsServerTestFixture : public gcs::GcsServer {
 public:
  using gcs::GcsServer::GcsServer;
  void TriggerPromotion() { DoStartLoadingDeferred(); }
};

/// Runs an isolated GCS server on a dedicated event loop thread. Used for
/// passive (multi-GCS) tests where multiple GcsServer instances run
/// concurrently. Each runner owns a private io_context + thread, and stops/joins the
/// thread before the server is deallocated to avoid use-after-free from pending
/// callbacks.
class DedicatedGcsServerRunner {
 public:
  DedicatedGcsServerRunner(const gcs::GcsServerConfig &config,
                           const gcs::GcsServerMetrics &metrics)
      : io_service_(),
        server_(std::make_unique<GcsServerTestFixture>(config, metrics, io_service_)) {}

  /// Starts the GCS server and its private event loop thread, blocking until the gRPC
  /// server has bound to its port and is ready to receive requests.
  void Start() {
    std::promise<int> port_promise;
    server_->SetPortReadyCallback(
        [&port_promise](int port) { port_promise.set_value(port); });

    thread_ = std::thread([this]() {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
          io_service_.get_executor());
      io_service_.run();
    });

    server_->Start();
    port_ = port_promise.get_future().get();
  }

  /// Stops the GCS server, stops its event loop, and joins the event thread. Must be
  /// called before deleting the server object.
  void Stop() {
    if (server_) {
      server_->Stop();
    }
    io_service_.stop();
    if (thread_.joinable()) {
      thread_.join();
    }
    server_.reset();
  }

  ~DedicatedGcsServerRunner() { Stop(); }

  GcsServerTestFixture &GetServer() { return *server_; }
  int GetPort() const { return port_; }

 private:
  instrumented_io_context io_service_;
  std::unique_ptr<GcsServerTestFixture> server_;
  std::thread thread_;
  int port_ = 0;
};

/// In-memory, thread-safe fake of the platform-agnostic lease client. It models a single
/// shared lease so multiple electors (in the same or different GcsServers) can contend
/// for leadership, and supports simulating grant/loss/crash for failover tests.
class MockLeaderLeaseClient : public gcs::LeaderLeaseClientInterface {
 public:
  MockLeaderLeaseClient() = default;

  Status TryAcquire(const std::string &holder_id,
                    int ttl_seconds,
                    std::string &current_leader) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (is_crashed_) {
      return Status::IOError("Crashed");
    }
    if (!leader_id_.empty() && leader_id_ != holder_id) {
      current_leader = leader_id_;
      return Status::OK();
    }
    if (should_grant_lease_) {
      leader_id_ = holder_id;
      current_leader = holder_id;
      return Status::OK();
    }
    current_leader = leader_id_;
    return Status::OK();
  }

  Status Renew(const std::string &holder_id,
               int ttl_seconds,
               std::string &current_leader) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (is_crashed_) {
      return Status::IOError("Crashed");
    }
    if (leader_id_ == holder_id) {
      if (should_lose_lease_) {
        leader_id_ = "";
        current_leader = "";
        return Status::OK();
      }
      current_leader = holder_id;
      return Status::OK();
    }
    current_leader = leader_id_;
    return Status::OK();
  }

  void Release(const std::string &holder_id) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (leader_id_ == holder_id) {
      leader_id_ = "";
    }
    release_count_++;
  }

  void SetGrantLease(bool grant) {
    std::lock_guard<std::mutex> lock(mutex_);
    should_grant_lease_ = grant;
  }

  void SetLoseLease(bool lose) {
    std::lock_guard<std::mutex> lock(mutex_);
    should_lose_lease_ = lose;
  }

  void SetCrashed(bool crashed) {
    std::lock_guard<std::mutex> lock(mutex_);
    is_crashed_ = crashed;
  }

  int GetReleaseCount() {
    std::lock_guard<std::mutex> lock(mutex_);
    return release_count_;
  }

  std::string GetLeaderId() {
    std::lock_guard<std::mutex> lock(mutex_);
    return leader_id_;
  }

 private:
  std::mutex mutex_;
  std::string leader_id_;
  bool should_grant_lease_ = false;
  bool should_lose_lease_ = false;
  bool is_crashed_ = false;
  int release_count_ = 0;
};

class GcsServerTest : public ::testing::Test {
 public:
  GcsServerTest()
      : fake_metrics_{
            /*actor_by_state_gauge=*/actor_by_state_gauge_,
            /*gcs_actor_by_state_gauge=*/gcs_actor_by_state_gauge_,
            /*running_job_gauge=*/running_job_gauge_,
            /*finished_job_counter=*/finished_job_counter_,
            /*job_duration_in_seconds_gauge=*/job_duration_in_seconds_gauge_,
            /*placement_group_gauge=*/placement_group_gauge_,
            /*placement_group_creation_latency_in_ms_histogram=*/
            placement_group_creation_latency_in_ms_histogram_,
            /*placement_group_scheduling_latency_in_ms_histogram=*/
            placement_group_scheduling_latency_in_ms_histogram_,
            /*placement_group_count_gauge=*/placement_group_count_gauge_,
            /*task_events_reported_gauge=*/task_events_reported_gauge_,
            /*task_events_dropped_gauge=*/task_events_dropped_gauge_,
            /*task_events_stored_gauge=*/task_events_stored_gauge_,
            /*event_recorder_dropped_events_counter=*/fake_dropped_events_counter_,
            /*storage_operation_latency_in_ms_histogram=*/
            storage_operation_latency_in_ms_histogram_,
            /*storage_operation_count_counter=*/storage_operation_count_counter_,
            /*resource_usage_gauge=*/fake_resource_usage_gauge_,
            fake_scheduler_placement_time_ms_histogram_,
            /*health_check_rpc_latency_ms_histogram=*/
            fake_health_check_rpc_latency_ms_histogram_,
            /*io_context_monitor_latency_ms_gauge=*/
            fake_io_context_monitor_latency_ms_gauge_,
            /*io_context_monitor_unhealthy_counter=*/
            fake_io_context_monitor_unhealthy_counter_,
        } {
    // A graceful Redis shutdown persists its dataset to dump.rdb (and appendonly.aof)
    // in the test's working directory. Since test cases run sequentially in the same
    // sandbox, a freshly started Redis would otherwise load the previous case's dataset
    // and leak stale GCS state (e.g. a duplicate head node). Remove them before/after
    // each test to guarantee a clean, isolated database.
    std::remove("dump.rdb");
    std::remove("appendonly.aof");
    TestSetupUtil::StartUpRedisServers(std::vector<int>());
  }

  virtual ~GcsServerTest() {
    TestSetupUtil::ShutDownRedisServers();
    // See the constructor: clear persisted Redis files so the next test starts clean.
    std::remove("dump.rdb");
    std::remove("appendonly.aof");
  }

  void SetUp() override {
    RayConfig::instance().io_context_monitor_healthy_deadline_ms() = 5000;

    gcs::GcsServerConfig config;
    config.grpc_server_port = 0;
    config.grpc_server_name = "MockedGcsServer";
    config.grpc_server_thread_num = 1;
    config.redis_address = "127.0.0.1";
    config.node_ip_address = "127.0.0.1";
    config.enable_sharding_conn = false;
    config.redis_port = TEST_REDIS_SERVER_PORTS.front();

    gcs_server_ = std::make_unique<gcs::GcsServer>(config, fake_metrics_, io_service_);
    std::promise<int> port_promise;
    gcs_server_->SetPortReadyCallback(
        [&port_promise](int port) { port_promise.set_value(port); });
    gcs_server_->Start();

    StartMainIOServiceThread();

    // Wait until server starts listening.
    int port = port_promise.get_future().get();

    // Create gcs rpc client
    client_call_manager_.reset(new rpc::ClientCallManager(
        io_service_, /*record_stats=*/false, /*local_address=*/""));
    client_.reset(new rpc::GcsRpcClient("0.0.0.0", port, *client_call_manager_));

    // Create health check stub.
    auto channel = grpc::CreateChannel("localhost:" + std::to_string(port),
                                       grpc::InsecureChannelCredentials());
    health_check_stub_ = grpc::health::v1::Health::NewStub(channel);
  }

  void TearDown() override {
    io_service_.stop();
    rpc::DrainServerCallExecutor();
    gcs_server_->Stop();
    if (thread_io_service_ && thread_io_service_->joinable()) {
      thread_io_service_->join();
    }
    gcs_server_.reset();
    rpc::ResetServerCallExecutor();
  }

  // Issues a health Check RPC and returns the reported serving status, or
  // std::nullopt if the RPC itself failed (e.g. timed out).
  std::optional<grpc::health::v1::HealthCheckResponse::ServingStatus> CheckHealth(
      std::chrono::milliseconds timeout) {
    grpc::health::v1::HealthCheckRequest request;
    grpc::health::v1::HealthCheckResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + timeout);
    auto status = health_check_stub_->Check(&context, request, &response);
    if (!status.ok()) {
      return std::nullopt;
    }
    return response.status();
  }

  // Polls the health check until it reports `expected` or the timeout elapses,
  // returning whether `expected` was observed.
  bool WaitForHealthStatus(grpc::health::v1::HealthCheckResponse::ServingStatus expected,
                           std::chrono::seconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    do {
      if (CheckHealth(std::chrono::milliseconds(1000)) == expected) {
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
  }

  void StartMainIOServiceThread() {
    thread_io_service_ = std::make_unique<std::thread>([this] {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
          io_service_.get_executor());
      io_service_.run();
    });
  }

  bool AddJob(rpc::AddJobRequest request) {
    std::promise<bool> promise;
    client_->AddJob(std::move(request),
                    [&promise](const Status &status, const rpc::AddJobReply &reply) {
                      RAY_CHECK_OK(status);
                      promise.set_value(true);
                    });
    return WaitReady(promise.get_future(), client_timeout_ms_);
  }

  bool MarkJobFinished(rpc::MarkJobFinishedRequest request) {
    std::promise<bool> promise;
    client_->MarkJobFinished(
        std::move(request),
        [&promise](const Status &status, const rpc::MarkJobFinishedReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), client_timeout_ms_);
  }

  std::optional<rpc::ActorTableData> GetActorInfo(const std::string &actor_id) {
    rpc::GetActorInfoRequest request;
    request.set_actor_id(actor_id);
    std::optional<rpc::ActorTableData> actor_table_data_opt;
    std::promise<bool> promise;
    client_->GetActorInfo(std::move(request),
                          [&actor_table_data_opt, &promise](
                              const Status &status, const rpc::GetActorInfoReply &reply) {
                            RAY_CHECK_OK(status);
                            if (reply.has_actor_table_data()) {
                              actor_table_data_opt = reply.actor_table_data();
                            } else {
                              actor_table_data_opt = std::nullopt;
                            }
                            promise.set_value(true);
                          });
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
    return actor_table_data_opt;
  }

  bool RegisterNode(rpc::RegisterNodeRequest request) {
    std::promise<bool> promise;
    client_->RegisterNode(
        std::move(request),
        [&promise](const Status &status, const rpc::RegisterNodeReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });

    return WaitReady(promise.get_future(), client_timeout_ms_);
  }

  bool UnregisterNode(rpc::UnregisterNodeRequest request) {
    std::promise<bool> promise;
    client_->UnregisterNode(
        std::move(request),
        [&promise](const Status &status, const rpc::UnregisterNodeReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });

    return WaitReady(promise.get_future(), client_timeout_ms_);
  }

  std::vector<rpc::GcsNodeInfo> GetAllNodeInfo() {
    std::vector<rpc::GcsNodeInfo> node_info_list;
    rpc::GetAllNodeInfoRequest request;
    std::promise<bool> promise;
    client_->GetAllNodeInfo(
        std::move(request),
        [&node_info_list, &promise](const Status &status,
                                    const rpc::GetAllNodeInfoReply &reply) {
          RAY_CHECK_OK(status);
          for (int index = 0; index < reply.node_info_list_size(); ++index) {
            node_info_list.push_back(reply.node_info_list(index));
          }
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
    return node_info_list;
  }

  bool ReportWorkerFailure(rpc::ReportWorkerFailureRequest request) {
    std::promise<bool> promise;
    client_->ReportWorkerFailure(
        std::move(request),
        [&promise](const Status &status, const rpc::ReportWorkerFailureReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(status.ok());
        });
    return WaitReady(promise.get_future(), client_timeout_ms_);
  }

  std::optional<rpc::WorkerTableData> GetWorkerInfo(const std::string &worker_id) {
    rpc::GetWorkerInfoRequest request;
    request.set_worker_id(worker_id);
    std::optional<rpc::WorkerTableData> worker_table_data_opt;
    std::promise<bool> promise;
    client_->GetWorkerInfo(
        std::move(request),
        [&worker_table_data_opt, &promise](const Status &status,
                                           const rpc::GetWorkerInfoReply &reply) {
          RAY_CHECK_OK(status);
          if (reply.has_worker_table_data()) {
            worker_table_data_opt = reply.worker_table_data();
          } else {
            worker_table_data_opt = std::nullopt;
          }
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
    return worker_table_data_opt;
  }

  std::vector<rpc::WorkerTableData> GetAllWorkerInfo() {
    std::vector<rpc::WorkerTableData> worker_table_data;
    rpc::GetAllWorkerInfoRequest request;
    std::promise<bool> promise;
    client_->GetAllWorkerInfo(
        std::move(request),
        [&worker_table_data, &promise](const Status &status,
                                       const rpc::GetAllWorkerInfoReply &reply) {
          RAY_CHECK_OK(status);
          for (int index = 0; index < reply.worker_table_data_size(); ++index) {
            worker_table_data.push_back(reply.worker_table_data(index));
          }
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), client_timeout_ms_));
    return worker_table_data;
  }

  bool AddWorkerInfo(rpc::AddWorkerInfoRequest request) {
    std::promise<bool> promise;
    client_->AddWorkerInfo(
        std::move(request),
        [&promise](const Status &status, const rpc::AddWorkerInfoReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), client_timeout_ms_);
  }

 protected:
  // Server-related fields.
  std::unique_ptr<gcs::GcsServer> gcs_server_;
  std::unique_ptr<std::thread> thread_io_service_;
  instrumented_io_context io_service_;

  // Client-related fields.
  std::unique_ptr<rpc::GcsRpcClient> client_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;
  std::unique_ptr<grpc::health::v1::Health::Stub> health_check_stub_;
  const std::chrono::milliseconds client_timeout_ms_{5000};

  // Fake metrics for testing
  observability::FakeGauge actor_by_state_gauge_;
  observability::FakeGauge gcs_actor_by_state_gauge_;
  observability::FakeGauge running_job_gauge_;
  observability::FakeCounter finished_job_counter_;
  observability::FakeGauge job_duration_in_seconds_gauge_;
  observability::FakeGauge placement_group_gauge_;
  observability::FakeHistogram placement_group_creation_latency_in_ms_histogram_;
  observability::FakeHistogram placement_group_scheduling_latency_in_ms_histogram_;
  observability::FakeGauge placement_group_count_gauge_;
  observability::FakeGauge task_events_reported_gauge_;
  observability::FakeGauge task_events_dropped_gauge_;
  observability::FakeGauge task_events_stored_gauge_;
  observability::FakeHistogram storage_operation_latency_in_ms_histogram_;
  observability::FakeCounter storage_operation_count_counter_;
  observability::FakeCounter fake_dropped_events_counter_;
  observability::FakeGauge fake_resource_usage_gauge_;
  observability::FakeHistogram fake_scheduler_placement_time_ms_histogram_;
  observability::FakeHistogram fake_health_check_rpc_latency_ms_histogram_;
  observability::FakeGauge fake_io_context_monitor_latency_ms_gauge_;
  observability::FakeCounter fake_io_context_monitor_unhealthy_counter_;

  // Fake metrics struct
  gcs::GcsServerMetrics fake_metrics_;
};

}  // namespace ray
