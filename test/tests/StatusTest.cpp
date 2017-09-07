#include <test/lib/BedrockTester.h>

struct StatusTest : tpunit::TestFixture {
    StatusTest()
        : tpunit::TestFixture("Status",
                              BEFORE_CLASS(StatusTest::setup),
                              TEST(StatusTest::test),
                              AFTER_CLASS(StatusTest::tearDown)) { }

    BedrockTester* tester;

    void setup() { tester = new BedrockTester(); }

    void tearDown() { delete tester; }

    void test() {
        SData status("Status");
        string response = tester->executeWaitMultipleData({status})[0].content;
        ASSERT_TRUE(SContains(response, "plugins"));
        ASSERT_TRUE(SContains(response, "multiWriteManualBlacklist"));
        ASSERT_TRUE(SContains(response, "multiWriteAutoBlacklist"));
    }

} __StatusTest;
