#include "catch.h"
#include "uri.h"

void test_uri(const std::string path_uri_str, const std::string protocol,  const std::string host, const int16_t nport, const std::string path, const std::string query) {
  uri path_uri(path_uri_str);

  INFO("URI=" << path_uri_str);
  CHECK_THAT(path_uri.protocol(), Equals(protocol));
  CHECK_THAT(path_uri.host(), Equals(host));
  CHECK(path_uri.nport() == nport);
  CHECK_THAT(path_uri.path(), Equals(path));
  CHECK_THAT(path_uri.query(), Equals(query));
}

TEST_CASE("Test uri parsing", "[uri]") {
  REQUIRE_THROWS(new uri(""));
  REQUIRE_THROWS(new uri("gibberish"));
  REQUIRE_THROWS(new uri("foo://xxx:9999999/dfdfd"));

  test_uri("hdfs://oda-master:9000/tmp", "hdfs", "oda-master", 9000, "/tmp", "");
  test_uri("hdfs://oda-master:9000/", "hdfs", "oda-master", 9000, "/", "");
  test_uri("hdfs://oda-master:9000", "hdfs", "oda-master", 9000, "", "");
  test_uri("hdfs://oda-master", "hdfs", "oda-master", 0, "", "");
  test_uri("hdfs://:9000", "hdfs", "", 9000, "", "");
  test_uri("hdfs://", "hdfs", "", 0, "", "");
  test_uri("hdfs:///", "hdfs", "", 0, "/", "");
  test_uri("hdfs:///tmp", "hdfs", "", 0, "/tmp", "");

  test_uri("s3://s3-bucket/tmp", "s3", "s3-bucket", 0, "/tmp", "");

  test_uri("gs://gcs-bucket/tmp", "gs", "gcs-bucket", 0, "/tmp", "");

  test_uri("fdfdfd://dfdfd/fdfdf?fdf", "fdfdfd", "dfdfd", 0, "/fdfdf", "fdf");
}

