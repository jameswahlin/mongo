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

#include "mongo/platform/basic.h"

#include <vector>

#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_mock.h"
#include "mongo/db/pipeline/document_source_unset.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace {

using std::vector;

//
// DocumentSourceUnset delegates much of its responsibilities to ParsedAddFields, which derives from
// ParsedAggregationProjection.
// Most of the functional tests are testing ParsedAddFields directly. These are meant as simpler
// integration tests.
//

// This provides access to getExpCtx(), but we'll use a different name for this test suite.
using UnsetTest = AggregationContextFixture;

TEST_F(UnsetTest, AcceptsValidUnsetSpec) {
    BSONObj spec = BSON("$unset" << BSON_ARRAY("a"
                                               << "b"
                                               << "c.d"));
    BSONElement specElement = spec.firstElement();
    [[maybe_unused]] auto stage = DocumentSourceUnset::createFromBson(specElement, getExpCtx());
}

TEST_F(UnsetTest, RejectsUnsetSpecWhichIsNotAnArray) {
    BSONObj spec = BSON("$unset"
                        << "foo");
    BSONElement specElement = spec.firstElement();
    ASSERT_THROWS_CODE(
        DocumentSourceUnset::createFromBson(specElement, getExpCtx()), AssertionException, 31002);
}

TEST_F(UnsetTest, RejectsUnsetSpecWithEmptyArray) {
    BSONObj spec = BSON("$unset" << BSONArray());
    BSONElement specElement = spec.firstElement();
    ASSERT_THROWS_CODE(
        DocumentSourceUnset::createFromBson(specElement, getExpCtx()), AssertionException, 31119);
}

TEST_F(UnsetTest, RejectsUnsetSpecWithArrayContainingAnyNonStringValue) {
    BSONObj spec = BSON("$unset" << BSON_ARRAY("a" << 2 << "b"));
    BSONElement specElement = spec.firstElement();
    ASSERT_THROWS_CODE(
        DocumentSourceUnset::createFromBson(specElement, getExpCtx()), AssertionException, 31120);
}

TEST_F(UnsetTest, UnsetSingleField) {
    auto updateDoc = BSON("$unset" << BSON_ARRAY("a"));
    auto project = DocumentSourceUnset::createFromBson(updateDoc.firstElement(), getExpCtx());
    auto source = DocumentSourceMock::createForTest({"{a: 10, b: 20}"});
    project->setSource(source.get());
    auto next = project->getNext();
    ASSERT(next.isAdvanced());
    ASSERT(next.getDocument().getField("a").missing());
    ASSERT_EQUALS(20, next.getDocument().getField("b").getInt());

    ASSERT(project->getNext().isEOF());
}

TEST_F(UnsetTest, UnsetMultipleFields) {
    auto updateDoc = BSON("$unset" << BSON_ARRAY("a"
                                                 << "b.c"
                                                 << "d.e"));
    auto project = DocumentSourceUnset::createFromBson(updateDoc.firstElement(), getExpCtx());
    auto source = DocumentSourceMock::createForTest({"{a: 10, b: {c: 20}, d: [{e: 30, f: 40}]}"});
    project->setSource(source.get());
    auto next = project->getNext();
    ASSERT(next.isAdvanced());
    ASSERT_BSONOBJ_EQ(next.getDocument().toBson(),
                      BSON("b" << BSONObj() << "d" << BSON_ARRAY(BSON("f" << 40))));

    ASSERT(project->getNext().isEOF());
}

TEST_F(UnsetTest, UnsetShouldBeAbleToProcessMultipleDocuments) {
    auto updateDoc = BSON("$unset" << BSON_ARRAY("a"));
    auto project = DocumentSourceUnset::createFromBson(updateDoc.firstElement(), getExpCtx());
    auto source = DocumentSourceMock::createForTest({"{a: 1, b: 2}", "{a: 3, b: 4}"});
    project->setSource(source.get());
    auto next = project->getNext();
    ASSERT(next.isAdvanced());
    ASSERT(next.getDocument().getField("a").missing());
    ASSERT_EQUALS(2, next.getDocument().getField("b").getInt());

    next = project->getNext();
    ASSERT(next.isAdvanced());
    ASSERT(next.getDocument().getField("a").missing());
    ASSERT_EQUALS(4, next.getDocument().getField("b").getInt());

    ASSERT(project->getNext().isEOF());
}

TEST_F(UnsetTest, UnsetShouldNotAddDependencies) {
    auto updateDoc = BSON("$unset" << BSON_ARRAY("a"
                                                 << "b.c"));
    auto project = DocumentSourceUnset::createFromBson(updateDoc.firstElement(), getExpCtx());

    DepsTracker dependencies;
    ASSERT_EQUALS(DepsTracker::State::SEE_NEXT, project->getDependencies(&dependencies));

    ASSERT_EQUALS(0U, dependencies.fields.size());
    ASSERT_EQUALS(false, dependencies.needWholeDocument);
    ASSERT_EQUALS(false, dependencies.getNeedsMetadata(DepsTracker::MetadataType::TEXT_SCORE));
}

TEST_F(UnsetTest, UnsetReportsExcludedPathsAsModifiedPaths) {
    auto updateDoc = BSON("$unset" << BSON_ARRAY("a"
                                                 << "b.c.d"
                                                 << "e.f.g"));
    auto project = DocumentSourceUnset::createFromBson(updateDoc.firstElement(), getExpCtx());

    auto modifiedPaths = project->getModifiedPaths();
    ASSERT(modifiedPaths.type == DocumentSource::GetModPathsReturn::Type::kFiniteSet);
    ASSERT_EQUALS(3U, modifiedPaths.paths.size());
    ASSERT_EQUALS(1U, modifiedPaths.paths.count("a"));
    ASSERT_EQUALS(1U, modifiedPaths.paths.count("b.c.d"));
    ASSERT_EQUALS(1U, modifiedPaths.paths.count("e.f.g"));
}

}  // namespace
}  // namespace mongo
