
db.coll.drop();
db.coll.insert({});
db.coll.createIndexes([
    {"obj.obj.obj.obj.str": 1, "obj.obj.obj.str": 1},
    {"obj.obj.obj.str": -1, "date": -1},
    {"obj.date": 1},
    {"obj.obj.obj.str": 1, "obj.obj.obj.obj.date": -1},
    {"obj.date": -1, "obj.obj.obj.str": 1},
    {"obj.date": 1, "obj.obj.num": 1},
    {"obj.obj.obj.str": 1, "obj.obj.obj.date": -1},
    {"obj.obj.obj.obj.str": 1, "obj.date": 1}
]);
var result =
    db.coll
        .aggregate([
            {
              $match: {
                  $and: [
                      {$or: [{'obj.obj.obj.str': {$not: {$in: []}}}, {'obj.date': {$nin: []}}]},
                      {'obj.obj.obj.obj.str': {$not: {$lte: ''}}}
                  ]
              }
            },
            {$project: {"obj.obj.obj.obj.str": 1, "_id": 0}}
        ])
        .toArray();
