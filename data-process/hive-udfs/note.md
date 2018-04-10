h1. Parameters and ObjectInspectors

| input | passed parameters | ObjectInspectors |
| --------------- | ---------------- | ---------------- |    
| select gudf_tests(1); | IntWritable@111ee2ca[value=1] | WritableConstantIntObjectInspector |
| select gudf_tests(1 + 2); | IntWritable@111ee2ca[value=1] | WritableConstantIntObjectInspector |
| select gudf_tests(1 + gc.id) from gc limit 1; | DoubleWritable@407612af[value=2.0] | WritableDoubleObjectInspector |




