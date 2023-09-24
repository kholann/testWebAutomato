# testWebAutomato
In this project I solved this task: https://docs.google.com/document/d/1MB9iSncgF46COGFOGsnwAHb1mexS2y-FVHeIequ9wOE/edit

The script convert_table.py in src uses OpenAI API.
This script takes a template table template.csv and a source data (table_A.csv or table_B.csv) and a target table name in the format:
convert_table.py —-source "source CSV" —-template "template CSV" —-target "target CSV"

As the output, the user receives the target table (for example, target.csv in data) in the Template format (columns, value formats) but with values from the source table.

Implementation notes:
1) I don't automatically generate data mapping code for each column display in the final Template format but use own function;
2) Check that all data has been transferred correctly by checksums in the source and target files. Now check only numbers of rows in the source and target;
3) Need to work with mapping types and formats in prompt message to receive python-style types and formats;
4) Use Levenstain distance for determining similarity tempate column names and source column names (for example, template.csv_table_A.csv_ratio.csv in data);
5) Use cosine similarity between embeddings of tempate column names and source column names (for example, template.csv_table_A.csv_cos_sim.csv in data).

My thoughts on edge cases and how they can be overcome:
1) If the source does not contain columns which are similar to template need to report it and stop work with the source;
2) If the source contains more than one column which are similar to template now choose the first option. May use Levenstain distance or cosine similarity for choosing option with the most ratio.
