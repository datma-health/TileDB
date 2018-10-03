#include "catch.h"
#include "url.h"

void test_url(const std::string path_url_str, const std::string protocol,  const std::string host, const int16_t nport, const std::string path, const std::string query) {
  url path_url(path_url_str);

  INFO("URL=" << path_url_str);
  CHECK_THAT(path_url.protocol(), Equals(protocol));
  CHECK_THAT(path_url.host(), Equals(host));
  CHECK(path_url.nport() == nport);
  CHECK_THAT(path_url.path(), Equals(path));
  CHECK_THAT(path_url.query(), Equals(query));
}


TEST_CASE("Test url parsing", "[url]") {
  test_url("hdfs://oda-master:9000/tmp", "hdfs", "oda-master", 9000, "/tmp", "");
  test_url("hdfs://oda-master:9000/", "hdfs", "oda-master", 9000, "/", "");
  test_url("hdfs://oda-master:9000", "hdfs", "oda-master", 9000, "", "");
  test_url("hdfs://oda-master", "hdfs", "oda-master", 0, "", "");
  test_url("hdfs://:9000", "hdfs", "", 9000, "", "");
  test_url("hdfs://", "hdfs", "", 0, "", "");
  test_url("hdfs:///", "hdfs", "", 0, "/", "");
  test_url("hdfs:///tmp", "hdfs", "", 0, "/tmp", "");

  test_url("s3://s3-bucket/tmp", "s3", "s3-bucket", 0, "/tmp", "");

  test_url("gs://gcs-bucket/tmp", "gs", "gcs-bucket", 0, "/tmp", "");

  test_url("fdfdfd://dfdfd/fdfdf?fdf", "fdfdfd", "dfdfd", 0, "/fdfdf", "fdf");
}

