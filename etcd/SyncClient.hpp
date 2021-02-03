#ifndef __ETCD_SYNC_CLIENT_HPP__
#define __ETCD_SYNC_CLIENT_HPP__

#include <limits>

#include "etcd/Client.hpp"

namespace etcd
{
  /**
   * SyncClient is a wrapper around etcd::Client and provides a simplified sync interface with blocking operations.
   *
   * In case of any exceptions occur in the backend the Response value's error_message will contain the
   * text of the exception and the error code will be 600
   *
   * Use this class only if performance does not matter.
   */
  class SyncClient
  {
  public:
    /**
     * Constructs an etcd sync client object.
     *
     * @param etcd_url is the url of the etcd server to connect to, like "http://127.0.0.1:2379",
     *                 or multiple url, seperated by ',' or ';'.
     * @param timeout timeout duration for each operation in terms of milliseconds
     * @param load_balancer is the load balance strategy, can be one of round_robin/pick_first/grpclb/xds.
     */
    SyncClient(std::string const & etcd_url,
               const long& timeout = std::numeric_limits<long>::max(),
               std::string const & load_balancer = "round_robin");

    /**
     * Constructs an etcd sync client object.
     *
     * @param etcd_url is the url of the etcd server to connect to, like "http://127.0.0.1:2379",
     *                 or multiple url, seperated by ',' or ';'.
     * @param username username of etcd auth
     * @param password password of etcd auth
     * @param timeout timeout duration for each operation in terms of milliseconds
     * @param load_balancer is the load balance strategy, can be one of round_robin/pick_first/grpclb/xds.
     */
    SyncClient(std::string const & etcd_url,
               std::string const & username,
               std::string const & password,
               const long& timeout = std::numeric_limits<long>::max(),
               std::string const & load_balancer = "round_robin");

    Response get(std::string const & key);
    Response set(std::string const & key, std::string const & value, int ttl = 0);
    Response set(std::string const & key, std::string const & value, int64_t leaseId);
    Response add(std::string const & key, std::string const & value, int ttl = 0);
    Response add(std::string const & key, std::string const & value, int64_t leaseId);
    Response modify(std::string const & key, std::string const & value, int ttl = 0);
    Response modify(std::string const & key, std::string const & value, int64_t leaseId);
    Response modify_if(std::string const & key, std::string const & value, std::string const & old_value, int ttl = 0);
    Response modify_if(std::string const & key, std::string const & value, std::string const & old_value, int64_t leaseId);
    Response modify_if(std::string const & key, std::string const & value, int old_index, int ttl = 0);
    Response modify_if(std::string const & key, std::string const & value, int old_index, int64_t leaseId);
    Response rm(std::string const & key);
    Response rm_if(std::string const & key, std::string const & old_value);
    Response rm_if(std::string const & key, int old_index);
    Response ls(std::string const & key);
    Response mkdir(std::string const & key, int ttl = 0);
    Response rmdir(std::string const & key, bool recursive = false);
    Response leasegrant(int ttl);
    Response leaserevoke(int64_t lease_id);
    Response leasetimetolive(int64_t lease_id);

    /**
     * Watches for changes of a key or a subtree. Please note that if you watch e.g. "/testdir" and
     * a new key is created, like "/testdir/newkey" then no change happened in the value of
     * "/testdir" so your watch will not detect this. If you want to detect addition and deletion of
     * directory entries then you have to do a recursive watch.
     * @param key is the value or directory to be watched
     * @param recursive if true watch a whole subtree
     */
    Response watch(std::string const & key, bool recursive = false);

    /**
     * Watches for changes of a key or a subtree from a specific index. The index value can be in the "past".
     * @param key is the value or directory to be watched
     * @param fromIndex the first index we are interested in
     * @param recursive if true watch a whole subtree
     */
    Response watch(std::string const & key, int fromIndex, bool recursive = false);

  protected:
    Client client;
  };
}

#endif
