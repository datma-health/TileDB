#include "catch.h"
#include "uri.h"
#include <unordered_map>

void test_uri(const std::string path_uri_str, const std::string protocol,  const std::string host, const int16_t nport, const std::string path, const std::unordered_map<std::string, std::string> query) {
  uri path_uri(path_uri_str);

  INFO("URI=" << path_uri_str);
  CHECK_THAT(path_uri.protocol(), Equals(protocol));
  CHECK_THAT(path_uri.host(), Equals(host));
  CHECK(path_uri.nport() == nport);
  CHECK_THAT(path_uri.path(), Equals(path));
  CHECK(path_uri.query().size() == query.size());
  for(auto pair: query){
    CHECK(path_uri.query().find(pair.first) != path_uri.query().end());
    CHECK_THAT(pair.second, Equals(path_uri.query()[pair.first]));
  }
}

TEST_CASE("Test uri parsing", "[uri]") {
  REQUIRE_THROWS(new uri(""));
  REQUIRE_THROWS(new uri("gibberish"));
  REQUIRE_THROWS(new uri("foo://xxx:9999999/dfdfd"));
  std::unordered_map<std::string, std::string> emptyMap;
  test_uri("hdfs://oda-master:9000/tmp", "hdfs", "oda-master", 9000, "/tmp", emptyMap);
  test_uri("hdfs://oda-master:9000/", "hdfs", "oda-master", 9000, "/", emptyMap);
  test_uri("hdfs://oda-master:9000", "hdfs", "oda-master", 9000, "", emptyMap);
  test_uri("hdfs://oda-master", "hdfs", "oda-master", 0, "", emptyMap);
  test_uri("hdfs://:9000", "hdfs", "", 9000, "", emptyMap);
  test_uri("hdfs://", "hdfs", "", 0, "", emptyMap);
  test_uri("hdfs:///", "hdfs", "", 0, "/", emptyMap);
  test_uri("hdfs:///tmp", "hdfs", "", 0, "/tmp", emptyMap);

  test_uri("s3://s3-bucket/tmp", "s3", "s3-bucket", 0, "/tmp", emptyMap);

  test_uri("gs://gcs-bucket/tmp", "gs", "gcs-bucket", 0, "/tmp", emptyMap);

  test_uri("fdfdfd://dfdfd/fdfdf?fdf=fdfdf", "fdfdfd", "dfdfd", 0, "/fdfdf", std::unordered_map<std::string, std::string>{
    {"fdf","fdfdf"}});
  test_uri("hdfs://oda-master:9000/tmp?testQuery=val&anotherQuery=anotherval&lastQuery=lastval", "hdfs", "oda-master", 9000, "/tmp", std::unordered_map<std::string, std::string>{
    {"testQuery","val"},{"anotherQuery","anotherval"},{"lastQuery","lastval"}});
  REQUIRE_THROWS(test_uri("hdfs://oda-master:9000/tmp?query&anotherquery=someval&otherquery&", "hdfs", "oda-master", 9000, "/tmp",emptyMap));
  REQUIRE_THROWS(test_uri("fdfdfd://dfdfd/fdfdf?firstQ=firstval&secondQ", "fdfdfd", "dfdfd", 0, "/fdfdf",emptyMap));
  REQUIRE_THROWS(test_uri("hdfs://oda-master:9000/tmp?query=someval&=otherquery", "hdfs", "oda-master", 9000, "/tmp",emptyMap));
  REQUIRE_THROWS(test_uri("hdfs://oda-master:9000/tmp?initialQuery=val&&anotherval&latterQuery=lastval", "hdfs", "oda-master", 9000, "/tmp",emptyMap));
  test_uri("hdfs://oda-master:9000/tmp?firstQuery=val&&secondquery=anotherval&thirdQuery=lastval", "hdfs", "oda-master", 9000, "/tmp", std::unordered_map<std::string, std::string>{
    {"firstQuery","val"},{"secondquery","anotherval"},{"thirdQuery","lastval"}});
  test_uri("hdfs://oda-master:9000/tmp?firstQuery=val&&secondquery=anotherval&thirdQuery=last%20val", "hdfs", "oda-master", 9000, "/tmp", std::unordered_map<std::string, std::string>{
    {"firstQuery","val"},{"secondquery","anotherval"},{"thirdQuery","last val"}});
  test_uri("hdfs://oda-master:9000/tmp?thirdQuery=someval123+otherval123==", "hdfs", "oda-master", 9000, "/tmp", std::unordered_map<std::string, std::string>{
    {"thirdQuery","someval123+otherval123=="}});
  test_uri("hdfs://oda-master:9000/tmp?firstQuery=this%20is%20a%20field&secondquery=was%20it%20clear%20%28already%29%3F", "hdfs", "oda-master", 9000, "/tmp", std::unordered_map<std::string, std::string>{
    {"firstQuery","this is a field"},{"secondquery","was it clear (already)?"}});
}

