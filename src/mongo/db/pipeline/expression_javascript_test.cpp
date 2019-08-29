/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/db/pipeline/expression_javascript.h"

#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/document_value_test_util.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "mongo/db/pipeline/process_interface_standalone.h"
#include "mongo/db/service_context_d_test_fixture.h"
#include "mongo/scripting/engine.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

class MapReduceFixture : public ServiceContextMongoDTest {
protected:
    MapReduceFixture()
        : _expCtx((new ExpressionContextForTest())), _vps(_expCtx->variablesParseState) {
        _expCtx->mongoProcessInterface = std::make_shared<MongoInterfaceStandalone>(_expCtx->opCtx);
    }

    boost::intrusive_ptr<ExpressionContextForTest>& getExpCtx() {
        return _expCtx;
    }

    const VariablesParseState& getVPS() {
        return _vps;
    }

    Variables* getVariables() {
        return &_expCtx->variables;
    }

private:
    void setUp() override;
    void tearDown() override;

    boost::intrusive_ptr<ExpressionContextForTest> _expCtx;
    VariablesParseState _vps;
};


void MapReduceFixture::setUp() {
    setTestCommandsEnabled(true);
    ServiceContextMongoDTest::setUp();
    ScriptEngine::setup();
}

void MapReduceFixture::tearDown() {
    ScriptEngine::dropScopeCache();
    ServiceContextMongoDTest::tearDown();
}

TEST_F(MapReduceFixture, ExpressionInternalJs) {
    auto bsonExpr = BSON("expr" << BSON("eval"
                                        << "function(first, second) {return first + second;};"
                                        << "args" << BSON_ARRAY(1 << 2)));

    auto expr = ExpressionInternalJs::parse(getExpCtx(), bsonExpr.firstElement(), getVPS());

    Value result = expr->evaluate({}, getVariables());
    std::cout << "JJ result: " << result << std::endl;

    ASSERT_VALUE_EQ(result, Value(3));
}

TEST_F(MapReduceFixture, ExpressionInternalJsEmit) {}

}  // namespace
}  // namespace mongo
