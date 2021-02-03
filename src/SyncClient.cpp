#include "etcd/SyncClient.hpp"

#define CHECK_EXCEPTIONS(cmd)                   \
  try                                           \
  {                                             \
    return cmd;                                 \
  }                                             \
  catch (std::exception const & ex)             \
  {                                             \
    return etcd::Response(500, ex.what());      \
  }

etcd::SyncClient::SyncClient(std::string const & address, const long& timeout,  std::string const & load_balancer)
  : client(address, timeout, load_balancer)
{
}

etcd::SyncClient::SyncClient(std::string const & address,
                             std::string const & username,
                             std::string const & password,
                             const long& timeout, 
                             std::string const & load_balancer)
  : client(address, username, password, timeout, load_balancer)
{
}

etcd::Response etcd::SyncClient::get(std::string const & key)
{
  CHECK_EXCEPTIONS(client.get(key).get());
}

etcd::Response etcd::SyncClient::set(std::string const & key, std::string const & value, int ttl)
{
  CHECK_EXCEPTIONS(client.set(key, value, ttl).get());
}

etcd::Response etcd::SyncClient::set(std::string const & key, std::string const & value, int64_t leaseId)
{
  CHECK_EXCEPTIONS(client.set(key, value, leaseId).get());
}

etcd::Response etcd::SyncClient::add(std::string const & key, std::string const & value, int ttl)
{
  CHECK_EXCEPTIONS(client.add(key, value, ttl).get());
}

etcd::Response etcd::SyncClient::add(std::string const & key, std::string const & value, int64_t leaseId)
{
  CHECK_EXCEPTIONS(client.add(key, value, leaseId).get());
}

etcd::Response etcd::SyncClient::modify(std::string const & key, std::string const & value, int ttl)
{
  CHECK_EXCEPTIONS(client.modify(key, value, ttl).get());
}

etcd::Response etcd::SyncClient::modify(std::string const & key, std::string const & value, int64_t leaseId)
{
  CHECK_EXCEPTIONS(client.modify(key, value, leaseId).get());
}

etcd::Response etcd::SyncClient::modify_if(std::string const & key, std::string const & value, std::string const & old_value, int ttl)
{
  CHECK_EXCEPTIONS(client.modify_if(key, value, old_value, ttl).get());
}

etcd::Response etcd::SyncClient::modify_if(std::string const & key, std::string const & value, std::string const & old_value, int64_t leaseId)
{
  CHECK_EXCEPTIONS(client.modify_if(key, value, old_value, leaseId).get());
}

etcd::Response etcd::SyncClient::modify_if(std::string const & key, std::string const & value, int old_index, int ttl)
{
  CHECK_EXCEPTIONS(client.modify_if(key, value, old_index, ttl).get());
}

etcd::Response etcd::SyncClient::modify_if(std::string const & key, std::string const & value, int old_index, int64_t leaseId)
{
  CHECK_EXCEPTIONS(client.modify_if(key, value, old_index, leaseId).get());
}

etcd::Response etcd::SyncClient::rm(std::string const & key)
{
  CHECK_EXCEPTIONS(client.rm(key).get());
}

etcd::Response etcd::SyncClient::rm_if(std::string const & key, std::string const & old_value)
{
  CHECK_EXCEPTIONS(client.rm_if(key, old_value).get());
}

etcd::Response etcd::SyncClient::rm_if(std::string const & key, int old_index)
{
  CHECK_EXCEPTIONS(client.rm_if(key, old_index).get());
}


etcd::Response etcd::SyncClient::rmdir(std::string const & key, bool recursive)
{
  CHECK_EXCEPTIONS(client.rmdir(key, recursive).get());
}

etcd::Response etcd::SyncClient::ls(std::string const & key)
{
  CHECK_EXCEPTIONS(client.ls(key).get());
}

etcd::Response etcd::SyncClient::leasegrant(int ttl)
{
  CHECK_EXCEPTIONS(client.leasegrant(ttl).get());
}

etcd::Response etcd::SyncClient::leaserevoke(int64_t lease_id)
{
  CHECK_EXCEPTIONS(client.leaserevoke(lease_id).get());
}

etcd::Response etcd::SyncClient::leasetimetolive(int64_t lease_id)
{
  CHECK_EXCEPTIONS(client.leasetimetolive(lease_id).get());
}

etcd::Response etcd::SyncClient::watch(std::string const & key, bool recursive)
{
  CHECK_EXCEPTIONS(client.watch(key, recursive).get());
}

etcd::Response etcd::SyncClient::watch(std::string const & key, int fromIndex, bool recursive)
{
  CHECK_EXCEPTIONS(client.watch(key, fromIndex, recursive).get());
}
