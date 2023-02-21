import json
from pyspark.sql.types import StructType
from pyspark.sql.types import *


schema= StructType([StructField('resourceType',StringType(),True),StructField('id',StringType(),True),\
StructField('meta',StructType([StructField('versionId',StringType(),True),StructField('lastUpdated',StringType(),True),\
StructField('profile',ArrayType(StringType()),True),StructField('Features',ArrayType(StringType()),True),StructField('healthdata',ArrayType(StringType()),True)]),True),\
StructField('extension',ArrayType(StructType([StructField('url',StringType(),True),\
StructField('valueCodeableConcept',\
StructType([StructField('coding',ArrayType(StructType([StructField('system',StringType(),True),\
StructField('code',StringType(),True),StructField('display',StringType(),True)])),True)]),True)])),True)])

# json_schema = json.dumps(schema.jsonValue(), indent=2)

# print(json_schema)

targetschemadict={"1":["resourceType", "id", "meta.versionId", "meta.lastUpdated", "meta.profile","meta.Features","meta.healthdata","extension"],'2':["new_extension.url","new_extension.valueCodeableConcept.coding"]}
explodedict={"1":["profile","Features","healthdata","extension"],"2":["coding"]}  

json_object = json.dumps(explodedict, indent = 4) 
print(json_object)





