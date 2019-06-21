
db.coll.drop();
db.coll.insert({});
db.coll.createIndex({"str": 1});

// var result = db.coll.find({'str': {$not: {$eq: ''}}}, {"str": 1, "_id": 0}).toArray();

var result = db.coll.find({'str': {$lt: MaxKey}}, {"str": 1, "_id": 0}).toArray();

assert.eq({}, result[0]);
