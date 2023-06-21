#include "catch.h"
#include "uri.h"
#include "vector"

void test_uri(const std::string path_uri_str, const std::string protocol,  const std::string host, const int16_t nport, const std::string path, const std::vector<std::pair<std::string, std::string>> query) {
  uri path_uri(path_uri_str);

  INFO("URI=" << path_uri_str);
  CHECK_THAT(path_uri.protocol(), Equals(protocol));
  CHECK_THAT(path_uri.host(), Equals(host));
  CHECK(path_uri.nport() == nport);
  CHECK_THAT(path_uri.path(), Equals(path));
  CHECK(path_uri.query().empty() == query.empty());
  for(int i = 0; i < query.size(); i++){
    CHECK_THAT(query[i].first,(Equals(path_uri.query()[i].first)));
    CHECK_THAT(query[i].second, Equals(path_uri.query()[i].second));
  }
}

TEST_CASE("Test uri parsing", "[uri]") {
  REQUIRE_THROWS(new uri(""));
  REQUIRE_THROWS(new uri("gibberish"));
  REQUIRE_THROWS(new uri("foo://xxx:9999999/dfdfd"));

  test_uri("hdfs://oda-master:9000/tmp", "hdfs", "oda-master", 9000, "/tmp", std::vector<std::pair<std::string, std::string>>());
  test_uri("hdfs://oda-master:9000/", "hdfs", "oda-master", 9000, "/", std::vector<std::pair<std::string, std::string>>());
  test_uri("hdfs://oda-master:9000", "hdfs", "oda-master", 9000, "", std::vector<std::pair<std::string, std::string>>());
  test_uri("hdfs://oda-master", "hdfs", "oda-master", 0, "", std::vector<std::pair<std::string, std::string>>());
  test_uri("hdfs://:9000", "hdfs", "", 9000, "", std::vector<std::pair<std::string, std::string>>());
  test_uri("hdfs://", "hdfs", "", 0, "", std::vector<std::pair<std::string, std::string>>());
  test_uri("hdfs:///", "hdfs", "", 0, "/", std::vector<std::pair<std::string, std::string>>());
  test_uri("hdfs:///tmp", "hdfs", "", 0, "/tmp", std::vector<std::pair<std::string, std::string>>());

  test_uri("s3://s3-bucket/tmp", "s3", "s3-bucket", 0, "/tmp", std::vector<std::pair<std::string, std::string>>());

  test_uri("gs://gcs-bucket/tmp", "gs", "gcs-bucket", 0, "/tmp", std::vector<std::pair<std::string, std::string>>());

  test_uri("fdfdfd://dfdfd/fdfdf?fdf=fdfdf", "fdfdfd", "dfdfd", 0, "/fdfdf", std::vector<std::pair<std::string, std::string>>({
    std::pair<std::string, std::string>("fdf","fdfdf")
  }));
  test_uri("hdfs://oda-master:9000/tmp?testQuery=val&anotherQuery=anotherval&lastQuery=lastval", "hdfs", "oda-master", 9000, "/tmp", std::vector<std::pair<std::string, std::string>>({
    std::pair<std::string, std::string>("testQuery","val"),std::pair<std::string, std::string>("anotherQuery","anotherval"),
    std::pair<std::string, std::string>("lastQuery","lastval")
  }));
  test_uri("hdfs://oda-master:9000/tmp?query&anotherquery=someval&otherquery&", "hdfs", "oda-master", 9000, "/tmp", std::vector<std::pair<std::string, std::string>>({
    std::pair<std::string, std::string>("anotherquery","someval")
  }));
}

