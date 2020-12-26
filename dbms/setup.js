var DBMS1 = "BJ";
var DBMS2 = "HK";

// zones
sh.addShardTag("beijing", DBMS1);
sh.addShardTag("hongkong", DBMS2);

sh.enableSharding("proj");

// set zone
sh.disableBalancing("proj.user")
sh.addTagRange("proj.user", {"region":"Beijing", "id":MinKey}, {"region":"Beijing", "id":MaxKey}, DBMS1);
sh.addTagRange("proj.user", {"region":"Hong Kong", "id":MinKey}, {"region":"Hong Kong", "id":MaxKey}, DBMS2);
sh.enableBalancing("proj.user");

sh.disableBalancing("proj.article")
sh.addTagRange("proj.article", {"category":"science", "id":MinKey}, {"category":"science", "id":MaxKey}, DBMS1);
sh.addTagRange("proj.article", {"category":"technology", "id":MinKey}, {"category":"technology", "id":MaxKey}, DBMS2);
sh.enableBalancing("proj.article");

sh.disableBalancing("proj.read")
sh.addTagRange("proj.read",{"region" : "Beijing", "id" : MinKey }, { "region" :"Beijing", "id" : MaxKey }, DBMS1);
sh.addTagRange("proj.read",{"region" : "Hong Kong", "id" : MinKey }, { "region" :"Hong Kong", "id" : MaxKey }, DBMS2);
sh.enableBalancing("proj.read");

sh.disableBalancing("proj.beread")
sh.addTagRange("proj.beread",{"category" : "science", "id" : MinKey }, { "category" :"science", "id" : MaxKey }, DBMS1);
sh.addTagRange("proj.beread",{"category" : "technology", "id" : MinKey }, { "category" :"technology", "id" : MaxKey }, DBMS2);
sh.enableBalancing("proj.beread");

// shard
sh.shardCollection("proj.user", {"region":1, "id":1});
sh.shardCollection("proj.article", {"category":1, "id":1});
sh.shardCollection("proj.read", {"region": 1, "id": 1});
sh.shardCollection("proj.beread", {"category": 1, "id": 1});

// index
conn = new Mongo();
db = conn.getDB("proj");
db.user.createIndex({"region":1, "id":1}, {unique:true})
db.article.createIndex({"category":1, "id":1}, {unique:true})
db.read.createIndex({"region":1, "id":1}, {unique:true})
db.beread.createIndex({"category":1, "id":1}, {unique:true})

print(sh.status());
