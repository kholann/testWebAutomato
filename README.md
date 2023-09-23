# testWebAutomato
The script convert_table.py in src uses OpenAI API.
This script takes a template table and a source data table (A or B) and a target table name in the format:
convert_table.py —-source "source CSV" —-template "template CSV" —-target "target CSV"

As the output, the user receives the target table (for example, target.csv in data) in the Template format (columns, value formats) but with values from the source table.

My thoughts on edge cases and how they can be overcome:
1) Check that all data has been transferred correctly by checksums in the source and target files. Now check only numbers of rows in the source and target;
3) Need to work with mapping types and formats in prompt message to receive python-style types and formats;
4) Use levenstain distance for determining similarity tempate column names and source column names (for example, template.csv_table_A.csv_ratio.csv in data);
5) Use cosinuse distance between embeddings of tempate column names and source column names (for example, template.csv_table_A.csv_cos_sim.csv in data).
