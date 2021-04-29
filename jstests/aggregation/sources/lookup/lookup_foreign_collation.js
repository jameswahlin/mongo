/**
 * Tests that $lookup respects the user-specified foreign pipeline collation.
 * @tags: [
 *   assumes_unsharded_collection,
 * ]
 */
load("jstests/aggregation/extras/utils.js");  // For anyEq.

(function() {

"use strict";

const testDB = db.getSiblingDB(jsTestName());
const localColl = testDB.local_no_collation;
const localCaseInsensitiveColl = testDB.local_collation;
const foreignColl = testDB.foreign_no_collation;
const foreignCaseInsensitiveColl = testDB.foreign_collation;

const caseInsensitiveCollation = {
    locale: "en_US",
    strength: 1
};

const simpleCollation = {
    locale: "simple"
};

function setup() {
    assert.commandWorked(testDB.runCommand({dropDatabase: 1}));

    assert.commandWorked(testDB.createCollection(localColl.getName()));
    assert.commandWorked(testDB.createCollection(foreignColl.getName()));
    assert.commandWorked(testDB.createCollection(localCaseInsensitiveColl.getName(),
                                                 {collation: caseInsensitiveCollation}));
    assert.commandWorked(testDB.createCollection(foreignCaseInsensitiveColl.getName(),
                                                 {collation: caseInsensitiveCollation}));

    assert.commandWorked(
        localColl.insert([{_id: "a"}, {_id: "b"}, {_id: "c"}, {_id: "d"}, {_id: "e"}]));
    assert.commandWorked(localCaseInsensitiveColl.insert(
        [{_id: "a"}, {_id: "b"}, {_id: "c"}, {_id: "d"}, {_id: "e"}]));
    assert.commandWorked(
        foreignColl.insert([{_id: "a"}, {_id: "B"}, {_id: "c"}, {_id: "D"}, {_id: "e"}]));
    assert.commandWorked(foreignCaseInsensitiveColl.insert(
        [{_id: "a"}, {_id: "B"}, {_id: "c"}, {_id: "D"}, {_id: "e"}]));
}

(function testCollationPermutations() {
    setup();

    // Pipeline style $lookup with cases insensitive collation.
    const lookupWithPipeline = (foreignColl) => {
        return {
         $lookup: {from: foreignColl.getName(), as: "foreignMatch", collation: caseInsensitiveCollation, let: {l_id: "$_id"}, pipeline: [{$match: {$expr: {$eq: ["$_id", "$$l_id"]}}}]}
     };
    };

    // Local-field foreign-field style $lookup with cases insensitive collation.
    const lookupWithLocalForeignField = (foreignColl) => {
        return {
         $lookup: {from: foreignColl.getName(), localField: "_id", foreignField: "_id", as: "foreignMatch", collation: caseInsensitiveCollation}
     };
    };

    const resultSetCaseInsensitive = [
        {_id: "a", foreignMatch: [{_id: "a"}]},
        {_id: "b", foreignMatch: [{_id: "B"}]},
        {_id: "c", foreignMatch: [{_id: "c"}]},
        {_id: "d", foreignMatch: [{_id: "D"}]},
        {_id: "e", foreignMatch: [{_id: "e"}]}
    ];

    const resultSetCaseSensitive = [
        {_id: "a", foreignMatch: [{_id: "a"}]},
        {_id: "b", foreignMatch: []},
        {_id: "c", foreignMatch: [{_id: "c"}]},
        {_id: "d", foreignMatch: []},
        {_id: "e", foreignMatch: [{_id: "e"}]}
    ];

    //
    // The following are the combinations of collation set on the local collection, foreign
    // collection, aggregate command and $lookup pipeline exercised by this test. "I" represents the
    // case insensitive collation.
    //
    //    Local Coll     |    Foreign Coll     | $lookup    |   Command
    //    ---------------|---------------------|------------|----------
    //      Default      |       Default       |  Default   |   Default
    //      Default      |       Default       |     I      |   Default
    //      Default      |          I          |     I      |   Default
    //         I         |          I          |     I      |   Default
    //         I         |       Default       |     I      |   Default
    //      Default      |       Default       |     I      |      I
    //      Default      |          I          |     I      |      I
    //         I         |          I          |     I      |      I
    //         I         |       Default       |     I      |      I
    //      Default      |       Default       |     I      |   Simple
    //      Default      |          I          |     I      |   Simple
    //         I         |          I          |     I      |   Simple
    //         I         |       Default       |     I      |   Simple
    //      Default      |       Default       |   Simple   |   Default
    //      Default      |          I          |   Simple   |   Default
    //         I         |          I          |   Simple   |   Default
    //         I         |       Default       |   Simple   |   Default
    //      Default      |       Default       |   Simple   |      I
    //      Default      |          I          |   Simple   |      I
    //         I         |          I          |   Simple   |      I
    //         I         |       Default       |   Simple   |      I
    //      Default      |       Default       |   Simple   |   Simple
    //      Default      |          I          |   Simple   |   Simple
    //         I         |          I          |   Simple   |   Simple
    //         I         |       Default       |   Simple   |   Simple

    // Executes an aggregation pipeline with both pipeline and localField/foreignField $lookup
    // syntax, exercising different combinations of collation setting. Asserts that the results
    // returned match those expected. Arguments:
    //    localColl: Local Collection
    //    foreignColl: Foreign Collection
    //    commandCollation: Collation set on the aggregate command. Pass null for default collation.
    //    lookupCollation: Collation specified in the $lookup stage.  Pass null for default
    //    collation. expectedResults: Results expected from the aggregate invocation
    //
    function assertExpectedResultSet(
        localColl, foreignColl, commandCollation, lookupCollation, expectedResults) {
        const lookupWithPipeline = {$lookup: {from: foreignColl.getName(), 
                                as: "foreignMatch",
                                let: {l_id: "$_id"}, 
                                pipeline: [{$match: {$expr: {$eq: ["$_id", "$$l_id"]}}}]}};

        const lookupWithLocalForeignField = {$lookup: {from: foreignColl.getName(), 
        localField: "_id", 
        foreignField: "_id", 
        as: "foreignMatch"}};

        if (lookupCollation) {
            lookupWithPipeline.$lookup.collation = lookupCollation;
            lookupWithLocalForeignField.$lookup.collation = lookupCollation;
        }

        const aggOptions = {};
        if (commandCollation) {
            aggOptions.collation = commandCollation;
        }

        let results = localColl.aggregate([lookupWithPipeline], aggOptions).toArray();
        assert(anyEq(results, expectedResults), tojson(results));

        results = localColl.aggregate([lookupWithLocalForeignField], aggOptions).toArray();
        assert(anyEq(results, expectedResults), tojson(results));
    }

    // Local Coll:      Default
    // Foreign Coll:    Default
    // Command:         Default
    // $lookup:         Default
    assertExpectedResultSet(localColl, foreignColl, null, null, resultSetCaseSensitive);

    // Local Coll:      Default
    // Foreign Coll:    Default
    // Command:         Default
    // $lookup:         Case Insensitive
    assertExpectedResultSet(
        localColl, foreignColl, null, caseInsensitiveCollation, resultSetCaseInsensitive);

    // Local Coll:      Default
    // Foreign Coll:    Case Insensitive
    // Command:         Default
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localColl,
                            foreignCaseInsensitiveColl,
                            null,
                            caseInsensitiveCollation,
                            resultSetCaseInsensitive);

    // Local Coll:      Case Insensitive
    // Foreign Coll:    Case Insensitive
    // Command:         Default
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localCaseInsensitiveColl,
                            foreignCaseInsensitiveColl,
                            null,
                            caseInsensitiveCollation,
                            resultSetCaseInsensitive);

    // Local Coll:      Case Insensitive
    // Foreign Coll:    Default
    // Command:         Default
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localCaseInsensitiveColl,
                            foreignColl,
                            null,
                            caseInsensitiveCollation,
                            resultSetCaseInsensitive);

    // Local Coll:      Default
    // Foreign Coll:    Default
    // Command:         Case Insensitive
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localColl,
                            foreignColl,
                            caseInsensitiveCollation,
                            caseInsensitiveCollation,
                            resultSetCaseInsensitive);

    // Local Coll:      Default
    // Foreign Coll:    Case Insensitive
    // Command:         Case Insensitive
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localColl,
                            foreignCaseInsensitiveColl,
                            caseInsensitiveCollation,
                            caseInsensitiveCollation,
                            resultSetCaseInsensitive);

    // Local Coll:      Case Insensitive
    // Foreign Coll:    Case Insensitive
    // Command:         Case Insensitive
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localCaseInsensitiveColl,
                            foreignCaseInsensitiveColl,
                            caseInsensitiveCollation,
                            caseInsensitiveCollation,
                            resultSetCaseInsensitive);

    // Local Coll:      Case Insensitive
    // Foreign Coll:    Default
    // Command:         Case Insensitive
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localCaseInsensitiveColl,
                            foreignColl,
                            caseInsensitiveCollation,
                            caseInsensitiveCollation,
                            resultSetCaseInsensitive);

    // Local Coll:      Default
    // Foreign Coll:    Default
    // Command:         Default
    // $lookup:         Case Insensitive
    assertExpectedResultSet(
        localColl, foreignColl, null, caseInsensitiveCollation, resultSetCaseInsensitive);

    // Local Coll:      Default
    // Foreign Coll:    Case Insensitive
    // Command:         Simple
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localColl,
                            foreignCaseInsensitiveColl,
                            simpleCollation,
                            caseInsensitiveCollation,
                            resultSetCaseInsensitive);

    // Local Coll:      Case Insensitive
    // Foreign Coll:    Case Insensitive
    // Command:         Simple
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localCaseInsensitiveColl,
                            foreignCaseInsensitiveColl,
                            simpleCollation,
                            caseInsensitiveCollation,
                            resultSetCaseInsensitive);

    // Local Coll:      Case Insensitive
    // Foreign Coll:    Default
    // Command:         Simple
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localCaseInsensitiveColl,
                            foreignColl,
                            simpleCollation,
                            caseInsensitiveCollation,
                            resultSetCaseInsensitive);

    // Local Coll:      Default
    // Foreign Coll:    Default
    // Command:         Default
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localColl, foreignColl, null, simpleCollation, resultSetCaseSensitive);

    // Local Coll:      Default
    // Foreign Coll:    Case Insensitive
    // Command:         Default
    // $lookup:         Case Insensitive
    assertExpectedResultSet(
        localColl, foreignCaseInsensitiveColl, null, simpleCollation, resultSetCaseSensitive);

    // Local Coll:      Case Insensitive
    // Foreign Coll:    Case Insensitive
    // Command:         Default
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localCaseInsensitiveColl,
                            foreignCaseInsensitiveColl,
                            null,
                            simpleCollation,
                            resultSetCaseSensitive);

    // Local Coll:      Case Insensitive
    // Foreign Coll:    Default
    // Command:         Default
    // $lookup:         Case Insensitive
    assertExpectedResultSet(
        localCaseInsensitiveColl, foreignColl, null, simpleCollation, resultSetCaseSensitive);

    // Local Coll:      Default
    // Foreign Coll:    Default
    // Command:         Case Insensitive
    // $lookup:         Case Insensitive
    assertExpectedResultSet(
        localColl, foreignColl, caseInsensitiveCollation, simpleCollation, resultSetCaseSensitive);

    // Local Coll:      Default
    // Foreign Coll:    Case Insensitive
    // Command:         Case Insensitive
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localColl,
                            foreignCaseInsensitiveColl,
                            caseInsensitiveCollation,
                            simpleCollation,
                            resultSetCaseSensitive);

    // Local Coll:      Case Insensitive
    // Foreign Coll:    Case Insensitive
    // Command:         Case Insensitive
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localCaseInsensitiveColl,
                            foreignCaseInsensitiveColl,
                            caseInsensitiveCollation,
                            simpleCollation,
                            resultSetCaseSensitive);

    // Local Coll:      Case Insensitive
    // Foreign Coll:    Default
    // Command:         Case Insensitive
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localCaseInsensitiveColl,
                            foreignColl,
                            caseInsensitiveCollation,
                            simpleCollation,
                            resultSetCaseSensitive);

    // Local Coll:      Default
    // Foreign Coll:    Default
    // Command:         Default
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localColl, foreignColl, null, simpleCollation, resultSetCaseSensitive);

    // Local Coll:      Default
    // Foreign Coll:    Case Insensitive
    // Command:         Simple
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localColl,
                            foreignCaseInsensitiveColl,
                            simpleCollation,
                            simpleCollation,
                            resultSetCaseSensitive);

    // Local Coll:      Case Insensitive
    // Foreign Coll:    Case Insensitive
    // Command:         Simple
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localCaseInsensitiveColl,
                            foreignCaseInsensitiveColl,
                            simpleCollation,
                            simpleCollation,
                            resultSetCaseSensitive);

    // Local Coll:      Case Insensitive
    // Foreign Coll:    Default
    // Command:         Simple
    // $lookup:         Case Insensitive
    assertExpectedResultSet(localCaseInsensitiveColl,
                            foreignColl,
                            simpleCollation,
                            simpleCollation,
                            resultSetCaseSensitive);
})();

(function testPipelineOptimizations() {
    setup();

    let pipeline = [{$lookup: {from: foreignColl.getName(), 
        as: "foreignMatch",
        let: {l_id: "$_id"}, 
        pipeline: [{$match: {$expr: {$eq: ["$_id", "$$l_id"]}}}],
        collation: caseInsensitiveCollation}},
        {$unwind: "$foreignMatch"},
        {$match: {"foreignMatch._id": "b"}}];

    let results = localColl.aggregate(pipeline).toArray();
    assert.eq(0, results.length);

    let explain = localColl.explain().aggregate(pipeline);
    let lastStage = explain.stages[explain.stages.length - 1];
    assert(lastStage.hasOwnProperty("$match"), tojson(explain));
    assert.eq({$match: {"foreignMatch._id": {$eq: "b"}}},
              lastStage,
              "The $match stage should not be optimized into the $lookup stage" + tojson(explain));

    pipeline = [{$lookup: {from: foreignColl.getName(), 
        as: "foreignMatch",
        let: {l_id: "$_id"}, 
        pipeline: [{$match: {$expr: {$eq: ["$_id", "$$l_id"]}}}],
        collation: caseInsensitiveCollation}},
        {$unwind: "$foreignMatch"},
        {$match: {"foreignMatch._id": "b"}}];

    let expectedResults = [{"_id": "b", "foreignMatch": {"_id": "B"}}];

    results = localColl.aggregate(pipeline, {collation: caseInsensitiveCollation}).toArray();
    assert(anyEq(results, expectedResults), tojson(results));

    explain = localColl.explain().aggregate(pipeline, {collation: caseInsensitiveCollation});
    lastStage = explain.stages[explain.stages.length - 1];
    assert(lastStage.hasOwnProperty("$lookup"), tojson(explain));
})();
})();
