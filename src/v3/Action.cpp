#include <grpc/support/log.h>
#include "etcd/v3/Action.hpp"

etcdv3::Action::Action(etcdv3::ActionParameters params)
{
  parameters = params;
  if (!parameters.auth_token.empty()) {
    // use `token` as the key, see:
    //
    //  etcd/etcdserver/api/v3rpc/rpctypes/metadatafields.go
    context.AddMetadata("token", parameters.auth_token);
  }
  start_timepoint = std::chrono::high_resolution_clock::now();
}

etcdv3::ActionParameters::ActionParameters()
{
  withPrefix = false;
  revision = 0;
  old_revision = 0;
  lease_id = 0;
  ttl = 0;
  kv_stub = NULL;
  watch_stub = NULL;
  lease_stub = NULL;
  timeout = std::numeric_limits<long>::max();
}

void etcdv3::Action::waitForResponse() 
{
  void* got_tag;
  bool ok = false;    

  auto status = cq_.AsyncNext(&got_tag, &ok, std::chrono::system_clock::now() + std::chrono::milliseconds(parameters.timeout));
  if( status == grpc::CompletionQueue::NextStatus::TIMEOUT )
  {
    ok = false;
    this->status = grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "Timeout");
  }
  else
  {
    GPR_ASSERT(got_tag == (void*)this);
  }
  
}

const std::chrono::high_resolution_clock::time_point etcdv3::Action::startTimepoint() {
  return this->start_timepoint;
}
