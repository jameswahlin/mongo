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

#include "mongo/db/pipeline/document_source_unset.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/pipeline/parsed_aggregation_projection.h"

namespace mongo {

using boost::intrusive_ptr;
using ParsedAggregationProjection = parsed_aggregation_projection::ParsedAggregationProjection;
using ProjectionPolicies = ParsedAggregationProjection::ProjectionPolicies;

REGISTER_DOCUMENT_SOURCE(unset,
                         LiteParsedDocumentSourceDefault::parse,
                         DocumentSourceUnset::createFromBson);

namespace {
BSONObj buildExclusionProjectionSpecification(const std::vector<BSONElement>& unsetSpec) {
    BSONObjBuilder objBuilder;
    for (const auto& elem : unsetSpec) {
        objBuilder << elem.valueStringData() << 0;
    }
    return objBuilder.obj();
}
}

intrusive_ptr<DocumentSource> DocumentSourceUnset::create(
    const std::vector<BSONElement>& unsetSpec, const intrusive_ptr<ExpressionContext>& expCtx) {

    uassert(31119,
            "$unset specification must be an array with at least one field",
            unsetSpec.size() > 0);

    uassert(31120,
            "$unset specification must be an array containing only string values",
            std::all_of(unsetSpec.cbegin(), unsetSpec.cend(), [](BSONElement elem) {
                return elem.type() == BSONType::String;
            }));


    const bool isIndependentOfAnyCollection = false;
    intrusive_ptr<DocumentSource> project(new DocumentSourceSingleDocumentTransformation(
        expCtx,
        ParsedAggregationProjection::create(
            expCtx,
            buildExclusionProjectionSpecification(unsetSpec),
            {ProjectionPolicies::DefaultIdPolicy::kIncludeId,
             ProjectionPolicies::ArrayRecursionPolicy::kRecurseNestedArrays}),
        DocumentSourceUnset::kStageName.rawData(),
        isIndependentOfAnyCollection));
    return project;
}

intrusive_ptr<DocumentSource> DocumentSourceUnset::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& expCtx) {
    uassert(31002, "$unset specification must be an array", elem.type() == BSONType::Array);
    return DocumentSourceUnset::create(elem.Array(), expCtx);
}

}  // namespace mongo
