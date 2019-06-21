
db.coll.drop();
db.coll.insert({});
db.coll.createIndex({"str": 1});

var result =
    db.coll.aggregate([{$match: {'str': {$not: {$eq: ''}}}}, {$project: {"str": 1, "_id": 0}}])
        .toArray();
assert.eq({}, result[0]);
