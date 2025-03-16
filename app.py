from pymongo import MongoClient
client=MongoClient()
print(client)
client=MongoClient("mongodb://localhost:27017/")

db=client['senario1']
collection=db['Transaction']


# Transaction=collection.find({},{"prix_unitaire":1,"type":1})
# print('list des Transaction')
# for t in Transaction:
#     print(f"prix_unitaire:{t['prix_unitaire']},type:{t['type']}")

# pipeline=[
#    {
#     "$group":{
#         "_id":"$type",
#     "moyenne":{"$avg":"$prix_unitaire"}
#     }}

# ]
# resultats=collection.aggregate(pipeline)
# for r in resultats:
#     print(f"type:{r['_id']},moyenne:{r['moyenne']}")


# res=collection.delete_many({"type":"vente"})
# print(f"nbr doc supr:{res.deleted_count}")

res=collection.update_many(
    {"type":"Vente"},
    {"$set":{"type":"sold"}}
    )
print(f"nbr doc modifie:{res.modified_count}")

client.close()