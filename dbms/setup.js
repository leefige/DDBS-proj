var DBMS1 = "BJ";
var DBMS2 = "HK";

// zones
sh.addShardTag("beijing", DBMS1);
sh.addShardTag("hongkong", DBMS2);

sh.enableSharding("proj");

// set zone
sh.disableBalancing("proj.user")
sh.addTagRange("proj.user", {"region":"Beijing", "uid":MinKey}, {"region":"Beijing", "uid":MaxKey}, DBMS1);
sh.addTagRange("proj.user", {"region":"Hong Kong", "uid":MinKey}, {"region":"Hong Kong", "uid":MaxKey}, DBMS2);
sh.enableBalancing("proj.user");

sh.disableBalancing("proj.article")
sh.addTagRange("proj.article", {"category":"science", "aid":MinKey}, {"category":"science", "aid":MaxKey}, DBMS1);
sh.addTagRange("proj.article", {"category":"science", "aid":MinKey}, {"category":"science", "aid":MaxKey}, DBMS2);
sh.addTagRange("proj.article", {"category":"technology", "aid":MinKey}, {"category":"technology", "aid":MaxKey}, DBMS2);
sh.enableBalancing("proj.article");

sh.disableBalancing("proj.read")
sh.addTagRange("proj.read",{"region" : "Beijing", "uid" : MinKey }, { "region" :"Beijing", "uid" : MaxKey }, DBMS1);
sh.addTagRange("proj.read",{"region" : "Hong Kong", "uid" : MinKey }, { "region" :"Hong Kong", "uid" : MaxKey }, DBMS2);
sh.enableBalancing("proj.read");

sh.disableBalancing("proj.beread")
sh.addTagRange("proj.beread",{"category" : "science", "aid" : MinKey }, { "category" :"science", "aid" : MaxKey }, DBMS1);
sh.addTagRange("proj.beread",{"category" : "technology", "aid" : MinKey }, { "category" :"technology", "aid" : MaxKey }, DBMS2);
sh.enableBalancing("proj.beread");

// shard
sh.shardCollection("proj.user", {"region":1, "uid":1});
sh.shardCollection("proj.article", {"category":1, "aid":1});
sh.shardCollection("proj.read", { region: 1, uid: 1 });
sh.shardCollection("proj.beread", { category: 1, aid: 1 });

print(sh.status());
