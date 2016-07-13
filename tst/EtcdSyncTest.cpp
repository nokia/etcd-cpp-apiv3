#include <catch.hpp>

#include "etcd/SyncClient.hpp"

static std::string etcd_uri("http://127.0.0.1:2379");

TEST_CASE("sync operations")
{
  etcd::SyncClient etcd(etcd_uri);
  etcd.rmdir("/test", true);

  // add
  CHECK(0 == etcd.add("/test/key1", "42").error_code());
  CHECK(105 == etcd.add("/test/key1", "42").error_code()); // Key already exists
  CHECK("42" == etcd.get("/test/key1").value().as_string());

  // modify
  CHECK(0 == etcd.modify("/test/key1", "43").error_code());
  CHECK(100 == etcd.modify("/test/key2", "43").error_code()); // Key not found
  CHECK("43" == etcd.modify("/test/key1", "42").prev_value().as_string());

  // set
  CHECK(0  == etcd.set("/test/key1", "43").error_code()); // overwrite
  CHECK(0  == etcd.set("/test/key2", "43").error_code()); // create new
  CHECK("43" == etcd.set("/test/key2", "44").prev_value().as_string());
  CHECK(""   == etcd.set("/test/key3", "44").prev_value().as_string());
  //CHECK(102  == etcd.set("/test",      "42").error_code()); // Not a file

  // rm
  CHECK(3 == etcd.ls("/test").keys().size());
  CHECK(0  == etcd.rm("/test/key1").error_code());
  CHECK(2 == etcd.ls("/test").keys().size());

  // mkdir
  //CHECK(etcd.mkdir("/test/new_dir").value().is_dir());

  // ls
  CHECK(0 == etcd.ls("/test/new_dir").keys().size());
  etcd.set("/test/new_dir/key1", "value1");
  etcd.set("/test/new_dir/key2", "value2");
  //etcd.mkdir("/test/new_dir/sub_dir");
  CHECK(2 == etcd.ls("/test/new_dir").keys().size());

  // rmdir
  //CHECK(108 == etcd.rmdir("/test/new_dir").error_code()); // Directory not empty
  CHECK(0 == etcd.rmdir("/test/new_dir", true).error_code());

  // compare and swap
  etcd.set("/test/key1", "42");
  int index = etcd.modify_if("/test/key1", "43", "42").index();
  CHECK(101 == etcd.modify_if("/test/key1", "44", "42").error_code());
  REQUIRE(etcd.modify_if("/test/key1", "44", index).is_ok());
  CHECK(101 == etcd.modify_if("/test/key1", "45", index).error_code());

  // atomic compare-and-delete based on prevValue
  etcd.set("/test/key1", "42");
  CHECK(101 == etcd.rm_if("/test/key1", "43").error_code());
  CHECK(0   == etcd.rm_if("/test/key1", "42").error_code());

  // atomic compare-and-delete based on prevIndex
  index = etcd.set("/test/key1", "42").index();
  CHECK(101 == etcd.rm_if("/test/key1", index - 1).error_code());
  CHECK(0   == etcd.rm_if("/test/key1", index).error_code());

  //leasegrant
  etcd::Response res = etcd.leasegrant(60);
  REQUIRE(res.is_ok());
  CHECK(60 == res.value().ttl());
  CHECK(0 <  res.value().lease());
  int64_t leaseid = res.value().lease();

  //add with lease
  res = etcd.add("/test/key1111", "43", leaseid);
  REQUIRE(0  == res.error_code()); // overwrite
  CHECK("create" == res.action());
  CHECK(leaseid ==  res.value().lease());

  //set with lease
  res = etcd.set("/test/key1", "43", leaseid);
  REQUIRE(0  == res.error_code());
  CHECK("set" == res.action());
  CHECK(leaseid ==  res.value().lease());

  //modify with lease
  res = etcd.modify("/test/key1", "44", leaseid);
  REQUIRE(0  == res.error_code()); 
  CHECK("update" == res.action());
  CHECK(leaseid ==  res.value().lease());
  CHECK("44" ==  res.value().as_string());

  res = etcd.modify_if("/test/key1", "45", "44", leaseid);
  index = res.index();
  REQUIRE(res.is_ok());
  CHECK("compareAndSwap" == res.action());
  CHECK(leaseid ==  res.value().lease());
  CHECK("45" == res.value().as_string());

  res = etcd.modify_if("/test/key1", "44", index, leaseid);
  index = res.index();
  REQUIRE(res.is_ok());
  CHECK("compareAndSwap" == res.action());
  CHECK(leaseid ==  res.value().lease());
  CHECK("44" == res.value().as_string());

// TEST_CASE("wait for a value change")
// {
//   etcd::Client etcd(etcd_uri);
//   etcd.set("/test/key1", "42").wait();

//   pplx::task<etcd::Response> res = etcd.watch("/test/key1");
//   CHECK(!res.is_done());

//   etcd.set("/test/key1", "43").get();
//   sleep(1);

//   REQUIRE(res.is_done());
//   REQUIRE("set" == res.get().action());
//   CHECK("43" == res.get().value().as_string());
// }

// TEST_CASE("wait for a directory change")
// {
//   etcd::Client etcd(etcd_uri);

//   pplx::task<etcd::Response> res = etcd.watch("/test", true);

//   etcd.add("/test/key4", "44").wait();
//   REQUIRE(res.is_done());
//   CHECK("create" == res.get().action());
//   CHECK("44" == res.get().value().as_string());

//   pplx::task<etcd::Response> res2 = etcd.watch("/test", true);

//   etcd.set("/test/key4", "45").wait();
//   sleep(1);
//   REQUIRE(res2.is_done());
//   CHECK("set" == res2.get().action());
//   CHECK("45" == res2.get().value().as_string());
// }

// TEST_CASE("watch changes in the past")
// {
//   etcd::Client etcd(etcd_uri);

//   int index = etcd.set("/test/key1", "42").get().index();

//   etcd.set("/test/key1", "43").wait();
//   etcd.set("/test/key1", "44").wait();
//   etcd.set("/test/key1", "45").wait();

//   etcd::Response res = etcd.watch("/test/key1", ++index).get();
//   CHECK("set" == res.action());
//   CHECK("43" == res.value().as_string());

//   res = etcd.watch("/test/key1", ++index).get();
//   CHECK("set" == res.action());
//   CHECK("44" == res.value().as_string());

//   res = etcd.watch("/test", ++index, true).get();
//   CHECK("set" == res.action());
//   CHECK("45" == res.value().as_string());
// }

// TEST_CASE("request cancellation")
// {
//   etcd::Client etcd(etcd_uri);
//   etcd.set("/test/key1", "42").wait();

//   pplx::task<etcd::Response> res = etcd.watch("/test/key1");
//   CHECK(!res.is_done());

//   etcd.cancel_operations();

//   sleep(1);
//   REQUIRE(res.is_done());
//   try
//   {
//     res.wait();
//   }
//   catch(pplx::task_canceled const & ex)
//   {
//     std::cout << "pplx::task_canceled: " << ex.what() << "\n";
//   }
//   catch(std::exception const & ex)
//   {
//     std::cout << "std::exception: " << ex.what() << "\n";
//   }
// }

  REQUIRE(0 == etcd.rmdir("/test", true).error_code());
}
