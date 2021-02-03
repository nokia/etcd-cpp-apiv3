#ifndef __V3_ACTION_HPP__
#define __V3_ACTION_HPP__

#include <chrono>
#include <limits>

#include <grpc++/grpc++.h>
#include "proto/rpc.grpc.pb.h"
#include "proto/v3lock.grpc.pb.h"

using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

using etcdserverpb::KV;
using etcdserverpb::Watch;
using etcdserverpb::Lease;
using v3lockpb::Lock;

namespace etcdv3
{
  enum Atomicity_Type
  {
    PREV_INDEX = 0,
    PREV_VALUE = 1
  };

  struct ActionParameters
  {
    ActionParameters();
    bool withPrefix;
    int revision;
    int old_revision;
    int64_t lease_id;
    int ttl;
    int limit;
    std::string key;
    std::string value;
    std::string old_value;
    std::string auth_token;
    KV::Stub* kv_stub;
    Watch::Stub* watch_stub;
    Lease::Stub* lease_stub;
    Lock::Stub* lock_stub;
    long timeout;
  };

  class Action
  {
  public:
    Action(etcdv3::ActionParameters params);
    void waitForResponse();
    const std::chrono::high_resolution_clock::time_point startTimepoint();
  protected:
    Status status;
    ClientContext context;
    CompletionQueue cq_;
    etcdv3::ActionParameters parameters;
    std::chrono::high_resolution_clock::time_point start_timepoint;
  };
}
#endif
