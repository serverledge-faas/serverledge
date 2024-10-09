package asl

/*
States.Format
This Intrinsic Function takes one or more arguments. The Value of the first MUST be a string, which MAY include zero or more instances of the character sequence {}. There MUST be as many remaining arguments in the Intrinsic Function as there are occurrences of {}. The interpreter returns the first-argument string with each {} replaced by the Value of the positionally-corresponding argument in the Intrinsic Function.

If necessary, the { and } characters can be escaped respectively as \\{ and \\}.

If the argument is a Path, applying it to the input MUST yield a value that is a string, a boolean, a number, or null. In each case, the Value is the natural string representation; string values are not accompanied by enclosing " characters. The Value MUST NOT be a JSON array or object.

For example, given the following Payload Template:

{
  "Parameters": {
    "foo.$": "States.Format('Your name is {}, we are in the year {}', $.name, 2020)"
  }
}
Suppose the input to the Payload Template is as follows:

{
  "name": "Foo",
  "zebra": "stripe"
}
After processing the Payload Template, the new payload becomes:

{
  "foo": "Your name is Foo, we are in the year 2020"
}
States.StringToJson
This Intrinsic Function takes a single argument whose Value MUST be a string. The interpreter applies a JSON parser to the Value and returns its parsed JSON form.

For example, given the following Payload Template:

{
  "Parameters": {
    "foo.$": "States.StringToJson($.someString)"
  }
}
Suppose the input to the Payload Template is as follows:

{
  "someString": "{\"number\": 20}",
  "zebra": "stripe"
}
After processing the Payload Template, the new payload becomes:

{
  "foo": {
    "number": 20
  }
}
States.JsonToString
This Intrinsic Function takes a single argument which MUST be a Path. The interpreter returns a string which is a JSON text representing the data identified by the Path.

For example, given the following Payload Template:

{
  "Parameters": {
    "foo.$": "States.JsonToString($.someJson)"
  }
}
Suppose the input to the Payload Template is as follows:

{
  "someJson": {
    "name": "Foo",
    "year": 2020
  },
  "zebra": "stripe"
}
After processing the Payload Template, the new payload becomes:

{
  "foo": "{\"name\":\"Foo\",\"year\":2020}"
}
States.Array
This Intrinsic Function takes zero or more arguments. The interpreter returns a JSON array containing the Values of the arguments, in the order provided.

For example, given the following Payload Template:

{
  "Parameters": {
    "foo.$": "States.Array('Foo', 2020, $.someJson, null)"
  }
}
Suppose the input to the Payload Template is as follows:

{
  "someJson": {
    "random": "abcdefg"
  },
  "zebra": "stripe"
}
After processing the Payload Template, the new payload becomes:

{
  "foo": [
    "Foo",
    2020,
    {
      "random": "abcdefg"
    },
    null
  ]
}
States.ArrayPartition
Use the States.ArrayPartition intrinsic function to partition a large array. You can also use this intrinsic to slice the data and then send the payload in smaller chunks.

This intrinsic function takes two arguments. The first argument is an array, while the second argument defines the chunk size. The interpreter chunks the input array into multiple arrays of the size specified by chunk size. The length of the last array chunk may be less than the length of the previous array chunks if the number of remaining items in the array is smaller than the chunk size.

Input validation

You must specify an array as the input value for the function's first argument.

You must specify a non-zero, positive integer for the second argument representing the chunk size value.

The input array can't exceed Step Functions' payload size limit of 256 KB.

For example, given the following input array:

{"inputArray": [1,2,3,4,5,6,7,8,9] }
You could use the States.ArrayPartition function to divide the array into chunks of four values:

"inputArray.$": "States.ArrayPartition($.inputArray,4)"
Which would return the following array chunks:

{"inputArray": [ [1,2,3,4], [5,6,7,8], [9]] }
In the previous example, the States.ArrayPartition function outputs three arrays. The first two arrays each contain four values, as defined by the chunk size. A third array contains the remaining value and is smaller than the defined chunk size.

States.ArrayContains
Use the States.ArrayContains intrinsic function to determine if a specific value is present in an array. For example, you can use this function to detect if there was an error in a Map state iteration.

This intrinsic function takes two arguments. The first argument is an array, while the second argument is the value to be searched for within the array.

Input validation

You must specify an array as the input value for function's first argument.
You must specify a valid JSON object as the second argument.
The input array can't exceed Step Functions' payload size limit of 256 KB.
For example, given the following input array:

{ "inputArray": [1,2,3,4,5,6,7,8,9], "lookingFor": 5 }
You could use the States.ArrayContains function to find the lookingFor value within the inputArray:

"contains.$": "States.ArrayContains($.inputArray, $.lookingFor)"
Because the value stored in lookingFor is included in the inputArray, States.ArrayContains returns the following result:

{"contains": true }
States.ArrayRange
Use the States.ArrayRange intrinsic function to create a new array containing a specific range of elements. The new array can contain up to 1000 elements.

This function takes three arguments. The first argument is the first element of the new array, the second argument is the final element of the new array, and the third argument is the increment value between the elements in the new array.

Input validation

You must specify integer values for all of the arguments.

You must specify a non-zero value for the third argument.

The newly generated array can't contain more than 1000 items.

For example, the following use of the States.ArrayRange function will create an array with a first value of 1, a final value of 9, and values in between the first and final values increase by two for each item:

"array.$": "States.ArrayRange(1, 9, 2)"
Which would return the following array:

{"array": [1,3,5,7,9] }
States.ArrayGetItem
This intrinsic function returns a specified index's value. This function takes two arguments. The first argument is an array of values and the second argument is the array index of the value to return.

For example, use the following inputArray and index values:

{ "inputArray": [1,2,3,4,5,6,7,8,9], "index": 5 }
From these values, you can use the States.ArrayGetItem function to return the value in the index position 5 within the array:

"item.$": "States.ArrayGetItem($.inputArray, $.index)"
In this example, States.ArrayGetItem would return the following result:

{ "item": 6 }
States.ArrayLength
The States.ArrayLength intrinsic function returns the length of an array. It has one argument, the array to return the length of.

For example, given the following input array:

{ "inputArray": [1,2,3,4,5,6,7,8,9] }
You can use States.ArrayLength to return the length of inputArray:

"length.$": "States.ArrayLength($.inputArray)"
In this example, States.ArrayLength would return the following JSON object that represents the array length:

{ "length": 9 }
States.ArrayUnique
The States.ArrayUnique intrinsic function removes duplicate values from an array and returns an array containing only unique elements. This function takes an array, which can be unsorted, as its sole argument.

For example, the following inputArray contains a series of duplicate values:

{"inputArray": [1,2,3,3,3,3,3,3,4] }
You could use the States.ArrayUnique function as and specify the array you want to remove duplicate values from:

"array.$": "States.ArrayUnique($.inputArray)"
The States.ArrayUnique function would return the following array containing only unique elements, removing all duplicate values:

{"array": [1,2,3,4] }
States.Base64Encode
Use the States.Base64Encode intrinsic function to encode data based on MIME Base64 encoding scheme. You can use this function to pass data to other AWS services without using an AWS Lambda function.

This function takes a data string of up to 10,000 characters to encode as its only argument.

For example, consider the following input string:

{"input": "Data to encode" }
You can use the States.Base64Encode function to encode the input string as a MIME Base64 string:

"base64.$": "States.Base64Encode($.input)"
The States.Base64Encode function returns the following encoded data in response:

{"base64": "RGF0YSB0byBlbmNvZGU=" }
States.Base64Decode
Use the States.Base64Decode intrinsic function to decode data based on MIME Base64 decoding scheme. You can use this function to pass data to other AWS services without using a Lambda function.

This function takes a Base64 encoded data string of up to 10,000 characters to decode as its only argument.

For example, given the following input:

{"base64": "RGF0YSB0byBlbmNvZGU=" }
You can use the States.Base64Decode function to decode the base64 string to a human-readable string:

"data.$": "States.Base64Decode($.base64)"
The States.Base64Decode function would return the following decoded data in response:

{"data": "Decoded data" }
States.Hash
Use the States.Hash intrinsic function to calculate the hash value of a given input. You can use this function to pass data to other AWS services without using a Lambda function.

This function takes two arguments. The first argument is the data you want to calculate the hash value of. The second argument is the hashing algorithm to use to perform the hash calculation. The data you provide must be an object string containing 10,000 characters or less.

The hashing algorithm you specify can be any of the following algorithms:

MD5
SHA-1
SHA-256
SHA-384
SHA-512
For example, you can use this function to calculate the hash value of the Data string using the specified Algorithm:

{ "Data": "input data", "Algorithm": "SHA-1" }
You can use the States.Hash function to calculate the hash value:

"output.$": "States.Hash($.Data, $.Algorithm)"
The States.Hash function returns the following hash value in response:

{"output": "aaff4a450a104cd177d28d18d7485e8cae074b7" }
States.JsonMerge
Use the States.JsonMerge intrinsic function to merge two JSON objects into a single object. This function takes three arguments. The first two arguments are the JSON objects that you want to merge.The third argument is a boolean value of false. This boolean value determines if the deep merging mode is enabled.

Currently, Step Functions only supports the shallow merging mode; therefore, you must specify the boolean value as false. In the shallow mode, if the same key exists in both JSON objects, the latter object's key overrides the same key in the first object. Additionally, objects nested within a JSON object aren't merged when you use shallow merging.

For example, you can use the States.JsonMerge function to merge the following JSON arrays that share the key a.

{ "json1": { "a": {"a1": 1, "a2": 2}, "b": 2, }, "json2": { "a": {"a3": 1, "a4": 2}, "c": 3 } }
You can specify the json1 and json2 arrays as inputs in the States.JasonMerge function to merge them together:

"output.$": "States.JsonMerge($.json1, $.json2, false)"
The States.JsonMerge returns the following merged JSON object as result. In the merged JSON object output, the json2 object's key a replaces the json1 object's key a. Also, the nested object in json1 object's key a is discarded because shallow mode doesn't support merging nested objects.

{ "output": { "a": {"a3": 1, "a4": 2}, "b": 2, "c": 3 } }
States.MathRandom
Use the States.MathRandom intrinsic function to return a random number between the specified start and end number. For example, you can use this function to distribute a specific task between two or more resources.

This function takes three arguments. The first argument is the start number, the second argument is the end number, and the last argument controls the seed value. The seed value argument is optional.

If you use this function with the same seed value, it returns an identical number.

Important

Because the States.MathRandom function doesn't return cryptographically secure random numbers, we recommend that you don't use it for security sensitive applications.

Input validation

You must specify integer values for the start number and end number arguments.
For example, to generate a random number from between one and 999, you can use the following input values:

{ "start": 1, "end": 999 }
To generate the random number, provide the start and end values to the States.MathRandom function:

"random.$": "States.MathRandom($.start, $.end)"
The States.MathRandom function returns the following random number as a response:

{"random": 456 }
States.MathAdd
Use the States.MathAdd intrinsic function to return the sum of two numbers. For example, you can use this function to increment values inside a loop without invoking a Lambda function.

Input validation

You must specify integer values for all the arguments.
For example, you can use the following values to subtract one from 111:

{ "value1": 111, "step": -1 }
Then, use the States.MathAdd function defining value1 as the starting value, and step as the value to increment value1 by:

"value1.$": "States.MathAdd($.value1, $.step)"
The States.MathAdd function would return the following number in response:

{"value1": 110 }
States.StringSplit
Use the States.StringSplit intrinsic function to split a string into an array of values. This function takes two arguments.The first argument is a string and the second argument is the delimiting character that the function will use to divide the string.

For example, you can use States.StringSplit to divide the following inputString, which contains a series of comma separated values:

{ "inputString": "1,2,3,4,5", "splitter": "," }
Use the States.StringSplit function and define inputString as the first argument, and the delimiting character splitter as the second argument:

"array.$": "States.StringSplit($.inputString, $.splitter)"
The States.StringSplit function returns the following string array as result:

{"array": ["1","2","3","4","5"] }
States.UUID
Use the States.UUID intrinsic function to return a version 4 universally unique identifier (v4 UUID) generated using random numbers. For example, you can use this function to call other AWS services or resources that need a UUID parameter or insert items in a DynamoDB table.

The States.UUID function is called with no arguments specified:

"uuid.$": "States.UUID()"
The function returns a randomly generated UUID, as in the following example:

{"uuid": "ca4c1140-dcc1-40cd-ad05-7b4aa23df4a8" }
*/
