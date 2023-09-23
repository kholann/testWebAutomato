# testWebAutomato
The script convert_table.py in src uses OpenAI API.
This script takes a template table and a source data table (A or B) and a target table name in the format:
convert_table.py —-source "source CSV" —-template "template CSV" —-target "target CSV"

As the output, the user receives the target table (for exmple, target.csv in data) in the Template format (columns, value formats) but with values from the source table.

My thoughts on edge cases and how they can be overcome:
1) I don't automatically generate data mapping code for each column display in the final Template format but used own function;
2) Check that all data has been transferred correctly by checksums in the source and target files. Now check only numbers of rows in the source and target;
3) Need to work with mapping types and formats in prompt message to receive python-style types and formats;
4) Used levenstain distance for determining similarity tempate column names and source column names (for exmple, template.csv_table_A.csv_ratio.csv in data);
5) Used cosinuse distance between embeddings of tempate column names and source column names (for exmple, template.csv_table_A.csv_cos_sim.csv in data).
