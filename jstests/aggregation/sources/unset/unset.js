// Basic testing for the $unset aggregation stage.
(function() {
    "use strict";

    const coll = db.agg_stage_unset;
    coll.drop();

    assert.commandWorked(coll.insert(
        [{_id: 0, a: 10}, {_id: 1, a: {b: 20, c: 30, 0: 40}}, {_id: 2, a: [{b: 50, c: 60}]}]));

    // unset single field.
    let result = coll.aggregate([{$sort: {_id: 1}}, {$unset: ["a"]}]).toArray();
    assert.eq(result, [{_id: 0}, {_id: 1}, {_id: 2}]);

    // unset multiple fields.
    result = coll.aggregate([{$sort: {_id: 1}}, {$unset: ["_id", "a"]}]).toArray();
    assert.eq(result, [{}, {}, {}]);

    // unset with dotted field path.
    result = coll.aggregate([{$sort: {_id: 1}}, {$unset: ["a.b"]}]).toArray();
    assert.eq(result, [{_id: 0, a: 10}, {_id: 1, a: {0: 40, c: 30}}, {_id: 2, a: [{c: 60}]}]);

    // Numeric field paths in aggregation represent field name only and not array offset.
    result = coll.aggregate([{$sort: {_id: 1}}, {$unset: ["a.0"]}]).toArray();
    assert.eq(result,
              [{_id: 0, a: 10}, {_id: 1, a: {b: 20, c: 30}}, {_id: 2, a: [{b: 50, c: 60}]}]);

})();
