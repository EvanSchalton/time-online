# time-online

Given an online directory containing files with following criteria:
- a numeric <strong>key</strong> (in the base case "user_id"),
- a categorical <strong>pivot</strong> variable (in the base case "path"), and
- a quantitative variable <strong>target</strong> (in the base case "length")
----
## Run
invoking `python script.py` will prompt you for several inputs:
- root_url: online directory containing csv files
- files: a comma separated list of the file names
- save_as: desired output file name, and the
- key
- pivot
- target, as defined above

## csv_gen
Stream srecords from each csv via a generator 

----
## download_data
Create an aggregated dictionary with the following structure:
```
{
    [key: int]: {
        [pivot: str]: total_target
    }
}
```

The download function returns two values:
1. the above result dictionary
2. A unique list of all of the <strong>pivot</strong> values

----
## save_data
Writes the results of the above download step to a csv, with rows sorted by the <strong>key</strong> in the first column and the <strong>pivot</strong> fields in alphabetical order
