# FHIR Framework for Flattening JSON Files

Welcome to the FHIR Framework for Flattening JSON Files! 

This framework is designed to make it easy to flatten JSON files that conform to the [FHIR standard](https://www.hl7.org/fhir/) into a more accessible format. By flattening the JSON, you can simplify the data and make it easier to work with, whether you're analyzing the data, building a user interface, or integrating it into another system.

## How to use the Framework

### Steps to use FHIR framework:

- Add Input Json file as Data file in Input_Files folder
- Configure the config.json file in COnfig_files folder

### Step to create COnfig File:
- Create your pyspark schema and then dump it in json format and then Paste it  under Source Schema field.
- For Example: This is example input json file for healthcare data
```
{
	"resourceType": "HealthcareService",
	"id": "283-hs04HealthCareServiceNewPatientAcceptingNeg",
	"meta": {
		"versionId": "1",
		"lastUpdated": "2022-08-25T14:56:40.090-05:00",
		"profile": [
			"http://bpd.bcbs.com/StructureDefinition/bpd-healthcareservice-plan-submission",
			"http://bpd.bcbs.com/StructureDefinition/"
		],
		"Features":[
			"This is trial",
			"Just trying",
			"Testing replicacy"
		],
		"healthdata":[
			"health check",
			"This is only check"
			
		]
	},
	"extension" : [
		{
		  "url" : "http://bpd.bcbs.com/StructureDefinition/geocodereturn",
		  "valueCodeableConcept" : {
			"coding" : [
			  {
				"system" : "http://bpd.bcbs.com/CodeSystem/BCBSGeocodeReturnCodeCS",
				"code" : "Z",
				"display" : "Street level"
			  }
			]
		  }
		},
		{
		  "url" : "http://bpd.bcbs.com/StructureDefinition/servicearea",
		  "valueCodeableConcept" : {
			"coding" : [
			  {
				"system" : "http://bpd.bcbs.com/CodeSystem/BCBSServiceAreaCS",
				"code" : "L",
				"display" : "Licensed"
			  }
			]
		  }
		},
		{
		  "url" : "http://bpd.bcbs.com/StructureDefinition/officelanguage",
		  "valueCodeableConcept" : {
			"coding" : [
			  {
				"system" : "urn:ietf:bcp:47",
				"code" : "en",
				"display" : "English"
			  }
			]
		  }
		},
		{
		  "url" : "http://bpd.bcbs.com/StructureDefinition/officelanguage",
		  "valueCodeableConcept" : {
			"coding" : [
			  {
				"system" : "urn:ietf:bcp:47",
				"code" : "it",
				"display" : "Italy"
			  }
			]
		  }
		}
	  ]
}

```
- It's Source schema would be:

```
{
        "type": "struct",
        "fields": [
          {
            "name": "resourceType",
            "type": "string",
            "nullable": true,
            "metadata": {}
          },
          {
            "name": "id",
            "type": "string",
            "nullable": true,
            "metadata": {}
          },
          {
            "name": "meta",
            "type": {
              "type": "struct",
              "fields": [
                {
                  "name": "versionId",
                  "type": "string",
                  "nullable": true,
                  "metadata": {}
                },
                {
                  "name": "lastUpdated",
                  "type": "string",
                  "nullable": true,
                  "metadata": {}
                },
                {
                  "name": "profile",
                  "type": {
                    "type": "array",
                    "elementType": "string",
                    "containsNull": true
                  },
                  "nullable": true,
                  "metadata": {}
                },
                {
                  "name": "Features",
                  "type": {
                    "type": "array",
                    "elementType": "string",
                    "containsNull": true
                  },
                  "nullable": true,
                  "metadata": {}
                },
                {
                  "name": "healthdata",
                  "type": {
                    "type": "array",
                    "elementType": "string",
                    "containsNull": true
                  },
                  "nullable": true,
                  "metadata": {}
                }
              ]
            },
            "nullable": true,
            "metadata": {}
          },
          {
            "name": "extension",
            "type": {
              "type": "array",
              "elementType": {
                "type": "struct",
                "fields": [
                  {
                    "name": "url",
                    "type": "string",
                    "nullable": true,
                    "metadata": {}
                  },
                  {
                    "name": "valueCodeableConcept",
                    "type": {
                      "type": "struct",
                      "fields": [
                        {
                          "name": "coding",
                          "type": {
                            "type": "array",
                            "elementType": {
                              "type": "struct",
                              "fields": [
                                {
                                  "name": "system",
                                  "type": "string",
                                  "nullable": true,
                                  "metadata": {}
                                },
                                {
                                  "name": "code",
                                  "type": "string",
                                  "nullable": true,
                                  "metadata": {}
                                },
                                {
                                  "name": "display",
                                  "type": "string",
                                  "nullable": true,
                                  "metadata": {}
                                }
                              ]
                            },
                            "containsNull": true
                          },
                          "nullable": true,
                          "metadata": {}
                        }
                      ]
                    },
                    "nullable": true,
                    "metadata": {}
                  }
                ]
              },
              "containsNull": true
            },
            "nullable": true,
            "metadata": {}
          }
        ]
      }
 ```

## Features

The FHIR Framework for Flattening JSON Files includes the following features:

- Easy-to-use codebase
- Customizable options for flattening the JSON data
- Support for FHIR standard data formats
- Comprehensive documentation and examples

## Contributing

We welcome contributions to the FHIR Framework for Flattening JSON Files! If you would like to contribute, please follow our [contribution guidelines](CONTRIBUTING.md) and submit a pull request.

## License

The FHIR Framework for Flattening JSON Files is licensed under the [MIT License](LICENSE).
