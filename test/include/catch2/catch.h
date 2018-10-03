#define CATCH_CONFIG_MAIN
#include "catch.hpp"

// Matchers
using Catch::Equals;
using Catch::StartsWith;
using Catch::EndsWith;
using Catch::Contains;
using Catch::Matches;

#define CHECK_RC(rc, expected) CHECK(rc == expected)
