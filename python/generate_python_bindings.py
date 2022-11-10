import re

scala_file_path = "../src/main/scala/com/databricks/labs/mosaic/functions/MosaicContext.scala"

with open(scala_file_path, "r") as f:
    scala_code = f.readlines()

# find where the functions object code definition starts in the scala code
# use regex match to find the start string, and zip with the index of each line to identify the
# line position in the code.
start_string = "object functions extends Serializable {"
start_index = (list(
    filter(
        lambda x: x[0],
        zip(
            [re.search(re.compile(start_string), x) for x in scala_code]
            , range(len(scala_code))
        )
    )
)[0][1])

# loop through the lines after the functions object start to find the closing brace
brace_counter = 1
adder = re.compile("{")
subtracter = re.compile("}")
for i in range(start_index, len(scala_code)):
    line = scala_code[i]
    brace_counter += len(re.findall(adder, line))
    brace_counter -= len(re.findall(subtracter, line))
    if brace_counter == 0:
        break

functions_object_block = scala_code[start_index: i]

# this regex will match the function signature. Include the start "def" and end "): Column" to guarentee catching end
# for example re.findall(function_pattern, "def f(x,\ny): Column\na load of code")
# ['def f(x,\ny):']
function_pattern = re.compile("(def [a-zA-Z_0-9]+\([0-9a-zA-Z_: , \n]+\)\: Column)")
functions = re.findall(function_pattern,"".join(functions_object_block))
# need to clean the signatures

def clean_signature(function_sig):
    cleaned = function_sig.replace("\n", " ") # replace newlines with spaces
    cleaned = cleaned.replace("def ", "") # remove the def
    cleaned = cleaned.replace("): Column", "")  # remove the trailing colon
    function_name, inputs = cleaned.split("(")
    inputs = [x.strip() for x in inputs.split(",")] # this will handle cases with differing number of spaces
    inputs = [(input_name.strip(), input_type.strip()) for input_name, input_type in [x.split(":") for x in inputs]]
    output = {"function_name": function_name, "inputs": inputs}
    return output

cleaned_functions = list(map(clean_signature, functions))

'''
Approach to handling overloaded functions
Build a dictionary like the below to hold all functions;
{
    function_name: {
        all_args: [union of (arg names, type) from all overloaded functions]
        ,function_name_{n}: [(arg_names, type) for that particular function]
        ,function_name_{n+1}: [(arg_names, type) for that particular function]
    }
}

Define each function once, with the full set of parameters. 
Determine the common parameters and make mandatory. All other parameters are optional. 
Use if statements to determine which parameters to pass to scala code. 

For example, with the below overloaded functions
f(x, y)
f(x, y, z)
f(x, y, z, w)

will generate:
def f(x, y, z=None, w=None):
    if x is not None and y is not None:
        f(x, y)
    if x is not None and y is not None and z is not None:
        f(x, y, z)
    if x is not None and y is not None and z is not None and w is not None:
        f(x, y, z, w)
 
'''

generated_function_data = {}
for function in cleaned_functions:
    f = function["function_name"]
    inputs = map(lambda x: x[0], function['inputs'])
    inputs = [x +': ColumnOrName' for x in inputs].copy() #it broke without this copy - don't remove it
    function['inputs'] = inputs
    # handle first time seen functions
    if not generated_function_data.get(f):
        generated_function_data[f] = {
            'all_args': inputs,
            'n_functions': 1,
            f'{f}_1'     : function

        }
    # now handle overloads - only for functions with different input args, rather than just types
    else:
        overloaded_info = generated_function_data.get(f)
        all_args = overloaded_info['all_args'].copy() # i'm not sure why this is necessary, but it is. Without it, previous entries will be overwritten
        # check to make sure there is an additional argument expected, not just a different type
        if max([x not in all_args for x in inputs]):
            all_args.extend(list(filter(lambda x: x not in all_args, inputs)))
            overloaded_info['all_args'] = all_args
            overloaded_info['n_functions'] +=1
            overloaded_info[f"{f}_{overloaded_info['n_functions']}"] = function

# now we can generate the function definition
bindings = []
for f in generated_function_data:
    # now go through and find the optional arguments. Can also define the subfunctions whilst at it
    function = generated_function_data[f]
    args_as_sets = []
    sub_functions = []
    # this builds the if statement that will call out to the scala code
    all_args =  function["all_args"]
    for k in function.keys():
        if k not in ['all_args', 'n_functions']:
            inputs = function[k]['inputs']
            args_as_sets.append(set(inputs))
            input_names = [x.replace(": ColumnOrName", " is not None") for x in inputs] # need an is not None for an if statement later on
            # need to add the remaining arguments as "is None" to ensure logical exclusivity
            exclusions = [x.replace(": ColumnOrName", " is None") for x in all_args if x not in inputs ]
            # add the exclusions
            input_names.extend(exclusions)
            conditionals = " and ".join(input_names)
            # wrap each input in col to make sure we're passing columns rather than single values (i.e. f(bool))
            java_call_outs = ', '.join([f'pyspark_to_java_column(boolAsColumn({x.replace(": ColumnOrName", "")}))' for x in inputs])
            pattern = f"""
    if {conditionals}:
        return config.mosaic_context.invoke_function(
        "{f}", {java_call_outs}
    )"""
            sub_functions.append(pattern)

    # find the args that are common across all functions - these will be mandatory for the function definition
    common_args = args_as_sets[0]
    for x in args_as_sets:
        common_args = common_args.intersection(x)
    # find the args that are not common across all functions - these will be optional for the function defition
    optional_args = set()
    for x in args_as_sets:
        optional_args.update(x.difference(common_args))

    # function signature
    function_name = f
    function_inputs = list(common_args)
    function_inputs.sort()
    if optional_args:
        optional_args = [x + " = None" for x in list(optional_args)]
        optional_args.sort()
        function_inputs.extend(optional_args)
    documented_inputs = "\t".join([x + "\n\n" for x in function_inputs])
    function_inputs = ", ".join(function_inputs)

    joined_sub_functions = '\n'.join(sub_functions)

    # this string needs to be indented the same as python code would be - hence the full left justification
    pattern = f"""
def {function_name}({function_inputs}) -> Column:
    \"""
    Please see https://databrickslabs.github.io/mosaic/ for full documentation

    Parameters
    ----------
    {documented_inputs}    
    Returns
    -------
    Column

    \"""
    {joined_sub_functions}

"""
    bindings.append(pattern)

headers = """
from pyspark.sql import Column
from pyspark.sql.functions import _to_java_column as pyspark_to_java_column

from mosaic.config import config
from mosaic.utils.types import ColumnOrName, boolAsColumn
"""

with open("mosaic/api/functions.py", "w") as f:
    f.writelines(headers)
    f.writelines(bindings)

print("bindings generated")