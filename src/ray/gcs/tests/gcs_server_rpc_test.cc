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

TEST_F(GcsServerTest, TestActorInfo) {
  // Create actor_table_data
  JobID job_id = JobID::FromInt(1);
  auto actor_table_data = GenActorTableData(job_id);
  // TODO(sand): Add tests that don't require checkponit.
}

TEST_F(GcsServerTest, TestJobInfo) {
  // Create job_table_data
  JobID job_id = JobID::FromInt(1);
  auto job_table_data = GenJobTableData(job_id);

  // Add job
  rpc::AddJobRequest add_job_request;
  add_job_request.mutable_data()->CopyFrom(*job_table_data);
  ASSERT_TRUE(AddJob(add_job_request));

  // Mark job finished
  rpc::MarkJobFinishedRequest mark_job_finished_request;
  mark_job_finished_request.set_job_id(job_table_data->job_id());
  ASSERT_TRUE(MarkJobFinished(mark_job_finished_request));
}

TEST_F(GcsServerTest, TestJobGarbageCollection) {
  // Create job_table_data
  JobID job_id = JobID::FromInt(1);
  auto job_table_data = GenJobTableData(job_id);

  // Add job
  rpc::AddJobRequest add_job_request;
  add_job_request.mutable_data()->CopyFrom(*job_table_data);
  ASSERT_TRUE(AddJob(add_job_request));

  auto actor_table_data = GenActorTableData(job_id);

  // Register detached actor for job
  auto detached_actor_table_data = GenActorTableData(job_id);
  detached_actor_table_data->set_is_detached(true);

  // Mark job finished
  rpc::MarkJobFinishedRequest mark_job_finished_request;
  mark_job_finished_request.set_job_id(job_table_data->job_id());
  ASSERT_TRUE(MarkJobFinished(mark_job_finished_request));

  std::function<bool()> condition_func = [this, &actor_table_data]() -> bool {
    return !GetActorInfo(actor_table_data->actor_id()).has_value();
  };
  ASSERT_TRUE(WaitForCondition(condition_func, 10 * 1000));
}

TEST_F(GcsServerTest, TestNodeInfo) {
  // Create gcs node info
  auto gcs_node_info = GenNodeInfo();

  // Register node info
  rpc::RegisterNodeRequest register_node_info_request;
  register_node_info_request.mutable_node_info()->CopyFrom(*gcs_node_info);
  ASSERT_TRUE(RegisterNode(register_node_info_request));
  std::vector<rpc::GcsNodeInfo> node_info_list = GetAllNodeInfo();
  ASSERT_EQ(node_info_list.size(), 1);
  ASSERT_EQ(node_info_list[0].state(), rpc::GcsNodeInfo::ALIVE);

  // Unregister node info
  rpc::UnregisterNodeRequest unregister_node_request;
  unregister_node_request.set_node_id(gcs_node_info->node_id());
  rpc::NodeDeathInfo node_death_info;
  node_death_info.set_reason(rpc::NodeDeathInfo::EXPECTED_TERMINATION);
  std::string reason_message = "Terminate node for testing.";
  node_death_info.set_reason_message(reason_message);
  unregister_node_request.mutable_node_death_info()->CopyFrom(node_death_info);
  ASSERT_TRUE(UnregisterNode(unregister_node_request));
  node_info_list = GetAllNodeInfo();
  ASSERT_EQ(node_info_list.size(), 1);
  ASSERT_TRUE(node_info_list[0].state() == rpc::GcsNodeInfo::DEAD);
  ASSERT_TRUE(node_info_list[0].death_info().reason() ==
              rpc::NodeDeathInfo::EXPECTED_TERMINATION);
  ASSERT_TRUE(node_info_list[0].death_info().reason_message() == reason_message);
}

TEST_F(GcsServerTest, TestNodeInfoFilters) {
  // Create gcs node info
  auto node1 = GenNodeInfo(1, "127.0.0.1", "node1");
  auto node2 = GenNodeInfo(2, "127.0.0.2", "node2");
  auto node3 = GenNodeInfo(3, "127.0.0.3", "node3");

  // Register node infos
  for (auto &node : {node1, node2, node3}) {
    rpc::RegisterNodeRequest register_node_info_request;
    register_node_info_request.mutable_node_info()->CopyFrom(*node);
    ASSERT_TRUE(RegisterNode(register_node_info_request));
  }

  // Kill node3
  rpc::UnregisterNodeRequest unregister_node_request;
  unregister_node_request.set_node_id(node3->node_id());
  rpc::NodeDeathInfo node_death_info;
  node_death_info.set_reason(rpc::NodeDeathInfo::EXPECTED_TERMINATION);
  std::string reason_message = "Terminate node for testing.";
  node_death_info.set_reason_message(reason_message);
  unregister_node_request.mutable_node_death_info()->CopyFrom(node_death_info);
  ASSERT_TRUE(UnregisterNode(unregister_node_request));

  {
    // Get all
    rpc::GetAllNodeInfoRequest request;
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));

    ASSERT_EQ(reply.node_info_list_size(), 3);
    ASSERT_EQ(reply.num_filtered(), 0);
    ASSERT_EQ(reply.total(), 3);
  }
  {
    // Get 2 by node id
    rpc::GetAllNodeInfoRequest request;
    request.add_node_selectors()->set_node_id(node1->node_id());
    request.add_node_selectors()->set_node_id(node2->node_id());
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));

    ASSERT_EQ(reply.node_info_list_size(), 2);
    ASSERT_EQ(reply.num_filtered(), 1);
    ASSERT_EQ(reply.total(), 3);
  }
  {
    // Get by state == ALIVE
    rpc::GetAllNodeInfoRequest request;
    request.set_state_filter(rpc::GcsNodeInfo::ALIVE);
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));

    ASSERT_EQ(reply.node_info_list_size(), 2);
    ASSERT_EQ(reply.num_filtered(), 1);
    ASSERT_EQ(reply.total(), 3);
  }

  {
    // Get by state == DEAD
    rpc::GetAllNodeInfoRequest request;
    request.set_state_filter(rpc::GcsNodeInfo::DEAD);
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));

    ASSERT_EQ(reply.node_info_list_size(), 1);
    ASSERT_EQ(reply.num_filtered(), 2);
    ASSERT_EQ(reply.total(), 3);
  }

  {
    // Get 2 by node_name
    rpc::GetAllNodeInfoRequest request;
    request.add_node_selectors()->set_node_name("node1");
    request.add_node_selectors()->set_node_name("node2");
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));

    ASSERT_EQ(reply.node_info_list_size(), 2);
    ASSERT_EQ(reply.num_filtered(), 1);
    ASSERT_EQ(reply.total(), 3);
  }

  {
    // Get 2 by node_ip_address
    rpc::GetAllNodeInfoRequest request;
    request.add_node_selectors()->set_node_ip_address("127.0.0.1");
    request.add_node_selectors()->set_node_ip_address("127.0.0.2");
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));

    ASSERT_EQ(reply.node_info_list_size(), 2);
    ASSERT_EQ(reply.num_filtered(), 1);
    ASSERT_EQ(reply.total(), 3);
  }

  {
    // Get 2 by node_id and node_name
    rpc::GetAllNodeInfoRequest request;
    request.add_node_selectors()->set_node_id(node1->node_id());
    request.add_node_selectors()->set_node_name("node2");
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));
    ASSERT_EQ(reply.node_info_list_size(), 2);
    ASSERT_EQ(reply.num_filtered(), 1);
    ASSERT_EQ(reply.total(), 3);
  }

  {
    // Get by node_id and state filter
    rpc::GetAllNodeInfoRequest request;
    request.add_node_selectors()->set_node_id(node1->node_id());
    request.add_node_selectors()->set_node_id(node3->node_id());
    request.set_state_filter(rpc::GcsNodeInfo::ALIVE);
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));
    ASSERT_EQ(reply.node_info_list_size(), 1);
    ASSERT_EQ(reply.num_filtered(), 2);
    ASSERT_EQ(reply.total(), 3);
  }

  {
    // Get by node_id, node_name and state filter
    rpc::GetAllNodeInfoRequest request;
    request.add_node_selectors()->set_node_id(node1->node_id());
    request.add_node_selectors()->set_node_name("node3");
    request.set_state_filter(rpc::GcsNodeInfo::DEAD);
    rpc::GetAllNodeInfoReply reply;
    RAY_CHECK_OK(client_->SyncGetAllNodeInfo(std::move(request), &reply));
    ASSERT_EQ(reply.node_info_list_size(), 1);
    ASSERT_EQ(reply.num_filtered(), 2);
    ASSERT_EQ(reply.total(), 3);
  }
}

TEST_F(GcsServerTest, TestWorkerInfo) {
  // Report worker failure
  auto worker_failure_data = GenWorkerTableData();
  worker_failure_data->mutable_worker_address()->set_ip_address("127.0.0.1");
  worker_failure_data->mutable_worker_address()->set_port(5566);
  rpc::ReportWorkerFailureRequest report_worker_failure_request;
  report_worker_failure_request.mutable_worker_failure()->CopyFrom(*worker_failure_data);
  ASSERT_TRUE(ReportWorkerFailure(report_worker_failure_request));
  std::vector<rpc::WorkerTableData> worker_table_data = GetAllWorkerInfo();
  ASSERT_EQ(worker_table_data.size(), 1);

  // Add worker info
  auto worker_data = GenWorkerTableData();
  worker_data->mutable_worker_address()->set_worker_id(WorkerID::FromRandom().Binary());
  rpc::AddWorkerInfoRequest add_worker_request;
  add_worker_request.mutable_worker_data()->CopyFrom(*worker_data);
  ASSERT_TRUE(AddWorkerInfo(add_worker_request));
  ASSERT_EQ(GetAllWorkerInfo().size(), 2);

  // Get worker info
  std::optional<rpc::WorkerTableData> result =
      GetWorkerInfo(worker_data->worker_address().worker_id());
  ASSERT_TRUE(result->worker_address().worker_id() ==
              worker_data->worker_address().worker_id());
}
// TODO(sang): Add tests after adding asyncAdd

TEST_F(GcsServerTest, HealthCheckSucceeds) {
  // The IOContextMonitor drives the serving status; poll until it reports SERVING.
  EXPECT_TRUE(WaitForHealthStatus(grpc::health::v1::HealthCheckResponse::SERVING,
                                  std::chrono::seconds(10)));
}

TEST_F(GcsServerTest, HealthCheckReflectsMainIOContextHealth) {
  // Healthy while the main io_context is running.
  ASSERT_TRUE(WaitForHealthStatus(grpc::health::v1::HealthCheckResponse::SERVING,
                                  std::chrono::seconds(10)));

  // Block the main io_context by occupying its (single-threaded) event loop with a
  // task that waits until released. The IOContextMonitor's probe can no longer
  // complete, so once it exceeds the healthy deadline the GCS reports NOT_SERVING.
  // The health check itself still responds since it runs on gRPC's own threads.
  // We block rather than stop the io_context so it keeps running on its original
  // thread (GCS components are pinned to it via thread checkers).
  std::promise<void> release;
  std::future<void> released = release.get_future();
  io_service_.post([&released]() { released.wait(); }, "BlockMainIOContextForTest");

  EXPECT_TRUE(WaitForHealthStatus(grpc::health::v1::HealthCheckResponse::NOT_SERVING,
                                  std::chrono::seconds(30)));

  // Release the io_context; probes complete again and the GCS recovers to SERVING.
  release.set_value();
  EXPECT_TRUE(WaitForHealthStatus(grpc::health::v1::HealthCheckResponse::SERVING,
                                  std::chrono::seconds(30)));
}

}  // namespace ray
