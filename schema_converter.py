import json
from pyspark.sql.types import StructType
from pyspark.sql.types import *


schema= StructType([StructField('resourceType',StringType(),True),StructField('id',StringType(),True),StructField('meta',StructType([StructField('versionId',StringType(),True),StructField('lastUpdated',StringType(),True),StructField('profile',ArrayType(StringType()),True)]),True),StructField('text',StructType([StructField('status',StringType(),True),StructField('div',StringType(),True)]),True),StructField('identifier',ArrayType(StructType([StructField('type',StructType([StructField('coding',ArrayType(StructType([StructField('system',StringType(),True),StructField('code',StringType(),True),StructField('display',StringType(),True)])),True)]),True),StructField('value',StringType(),True)])),True),StructField('name',ArrayType(StructType([StructField('family',StringType(),True),StructField('given',ArrayType(StringType()),True),StructField('suffix',ArrayType(StringType()),True)])),True),StructField('telecom',ArrayType(StructType([StructField('system',StringType(),True),StructField('value',StringType(),True),StructField('use',StringType(),True)])),True),StructField('gender',StringType(),True),StructField('qualification',ArrayType(StructType([StructField('extension',ArrayType(StructType([StructField('url',StringType(),True),StructField('valueCode',StringType(),True)])),True),StructField('code',StructType([StructField('extension',ArrayType(StructType([StructField('url',StringType(),True),StructField('valueBoolean',BooleanType(),True)])),True),StructField('coding',ArrayType(StructType([StructField('system',StringType(),True),StructField('code',StringType(),True),StructField('display',StringType(),True)])),True)]),True),StructField('issuer',StructType([StructField('reference',StringType(),True)]),True)])),True),StructField('communication',ArrayType(StructType([StructField('coding',ArrayType(StructType([StructField('system',StringType(),True),StructField('code',StringType(),True),StructField('display',StringType(),True)])),True)])),True)])

json_schema = json.dumps(schema.jsonValue(), indent=2)

print(json_schema)

targetschemadict={"1":["resourceType", "id", "meta.versionId", "meta.lastUpdated", "meta.profile","meta.Features","meta.healthdata","extension"],'2':["new_extension.url","new_extension.valueCodeableConcept.coding"]}
explodedict={"1":["profile","Features","healthdata","extension"],"2":["coding"]}  

# json_object = json.dumps(schema, indent = 4) 
# print(json_object)





