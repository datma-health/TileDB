#include "catch.h"
#include "uri.h"

void test_uri(const std::string path_uri_str, const std::string protocol,  
    const std::string host, const int16_t nport, const std::string path, 
    const std::string query, const std::string endpoint) {
  uri path_uri(path_uri_str);

  INFO("URI=" << path_uri_str);
  CHECK_THAT(path_uri.protocol(), Equals(protocol));
  CHECK_THAT(path_uri.host(), Equals(host));
  CHECK(path_uri.nport() == nport);
  CHECK_THAT(path_uri.path(), Equals(path));
  CHECK_THAT(path_uri.query(), Equals(query));
  CHECK_THAT(path_uri.endpoint(), Equals(endpoint));
}

TEST_CASE("Test uri parsing", "[uri]") {
  REQUIRE_THROWS(new uri(""));
  REQUIRE_THROWS(new uri("gibberish"));
  REQUIRE_THROWS(new uri("foo://xxx:9999999/dfdfd"));

  test_uri("hdfs://oda-master:9000/tmp", "hdfs", "oda-master", 9000, "/tmp", "", "");
  test_uri("hdfs://oda-master:9000/", "hdfs", "oda-master", 9000, "/", "", "");
  test_uri("hdfs://oda-master:9000", "hdfs", "oda-master", 9000, "", "", "");
  test_uri("hdfs://oda-master", "hdfs", "oda-master", 0, "", "", "");
  test_uri("hdfs://:9000", "hdfs", "", 9000, "", "", "");
  test_uri("hdfs://", "hdfs", "", 0, "", "", "");
  test_uri("hdfs:///", "hdfs", "", 0, "/", "", "");
  test_uri("hdfs:///tmp", "hdfs", "", 0, "/tmp", "", "");

  test_uri("s3://s3-bucket/tmp", "s3", "s3-bucket", 0, "/tmp", "", "");

  test_uri("gs://gcs-bucket/tmp", "gs", "gcs-bucket", 0, "/tmp", "", "");

  test_uri("fdfdfd://dfdfd/fdfdf?fdf", "fdfdfd", "dfdfd", 0, "/fdfdf", "fdf", "");
  test_uri("az://odastgdev.blob.core.windows.net/test", "az", "odastgdev.blob.core.windows.net", 0, "/test", "", "core.windows.net"); 
}

void test_azb_uri(const std::string path_uri_str, const std::string account,  const std::string endpoint) {
  azure_uri azb_uri(path_uri_str);

  INFO("Azure azb:// URI=" << path_uri_str);
  CHECK_THAT(azb_uri.account(), Equals(account));
  CHECK_THAT(azb_uri.endpoint(), Equals(endpoint));
}

TEST_CASE("Test azb:// uri parsing", "[azb_uri]") {
  REQUIRE_THROWS(new azure_uri(""));
  REQUIRE_THROWS(new azure_uri("kms://asdfsadfd"));
  test_azb_uri("azb://?account=odastgdev", "odastgdev", "");
  test_azb_uri("azb://?endpoint=core.windows.net", "", "core.windows.net");
  test_azb_uri("azb://?account=odastgdev&endpoint=core.windows.net", "odastgdev", "core.windows.net");
  test_azb_uri("azb://?endpoint=core.windows.net&account=odastgdev", "odastgdev", "core.windows.net");
}
