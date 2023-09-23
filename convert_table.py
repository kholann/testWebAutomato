import os
import openai
import argparse
import pandas as pd
from fuzzywuzzy import fuzz
from langchain.embeddings import OpenAIEmbeddings
from openai.embeddings_utils import cosine_similarity
import json

os.environ["OPENAI_API_KEY"] = "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"
openai.api_key = os.getenv("OPENAI_API_KEY")

def get_column_names(table):
    df = pd.read_csv(table)
    df_columns = df.columns.tolist()
    return df_columns

def find_name_column_in_dict_data(data, name_column):
    for key_value, value_value in data.items():
        if str(name_column).strip() == str(value_value).strip() or str(name_column).strip() == str(key_value).strip():
            return True
    return False

def map_columns(template, source, mapping_file, target):
    df_template = pd.read_csv(template)
    template_columns = df_template.columns.tolist()

    df_source = pd.read_csv(source)
    numb_rows_source = df_source.shape[0]

    template_dict_list = []

    with open(mapping_file) as json_file:
        data = json.load(json_file)
        for i in range(0, len(template_columns)):
            if type(data) == dict:
                for key, value in data.items():
                    if type(value) == list:
                        for j in range(0, len(value)):
                            if type(value[j]) == dict:
                                if find_name_column_in_dict_data(value[j], template_columns[i]):
                                    template_dict_list.append(value[j])
            if type(data) == list:
                for j in range(0, len(data)):
                    if type(data[j]) == dict:
                        if find_name_column_in_dict_data(data[j], template_columns[i]):
                            template_dict_list.append(data[j])

    df_source_need = pd.DataFrame()

    #TODO: work with mapping types
    type_format_map = {}
    type_format_map['integer'] = 'int'
    type_format_map['string'] = 'str'

    date_format_map = {}
    date_format_map['dd-mm-yyyy'] = '%d-%m-%Y'
    date_format_map['mm/dd/yyyy'] = '%m/%d/%Y'
    date_format_map['yyyy-mm-dd'] = '%Y-%m-%d'

    for i in range(0, len(template_dict_list)):
        df_source_need[[template_dict_list[i]['template_column']]] = df_source[[template_dict_list[i]['source_column']]]
        template_data_format = str(template_dict_list[i]['template_data_format']).strip().lower()
        source_data_format = str(template_dict_list[i]['source_data_format']).strip().lower()
        template_column = template_dict_list[i]['template_column']
        source_column = template_dict_list[i]['source_column']

        if (template_data_format != source_data_format):
            if find_name_column_in_dict_data(date_format_map, template_data_format):
                if find_name_column_in_dict_data(date_format_map, source_data_format):
                    df_source_need[template_column] = pd.to_datetime(df_source[source_column], format=date_format_map[source_data_format])
                    df_source_need[template_column] = df_source_need[template_column].apply(lambda x: x.strftime(date_format_map[template_data_format]))
            try:
                map_format = type_format_map[template_data_format]
                df_source_need[[template_column]].astype(map_format)
            except:
                print(f'No format {template_data_format} or {source_data_format}.')

    numb_rows_target = df_source_need.shape[0]
    if (numb_rows_source == numb_rows_target):
        print(f'All data from {source} has been transferred correctly to {target}.')
    else:
        print(f'Not all data from {source} (number of rows {numb_rows_source}) has been transferred correctly to {target} (number of rows {numb_rows_target}).')

    df_source_need.to_csv(target)

def match_csv_file_columns_with_levenstain_dist(template, source):
    column_mapping = template.replace(".txt", "") + "_" + source.replace(".txt", "") + "_ratio.csv"

    with open(column_mapping, 'w') as mapping_file:
        mapping_file.write("template;source;ratio"+"\n")
        template_read = get_column_names(template)
        source_read = get_column_names(source)
        for i in range(0, len(template_read)):
            max_ratio = 0
            source_name_column = ""
            for j in range(0, len(source_read)):
                current_ratio = fuzz.ratio(template_read[i], source_read[j])
                if current_ratio > max_ratio:
                    max_ratio = current_ratio
                    source_name_column = source_read[j]
            mapping_file.write(str(template_read[i]).strip() + ";" + str(source_name_column).strip() + ";" + str(max_ratio) + "\n")

    return column_mapping

def find_embeddings(template_read, source_read):
    embeddings = OpenAIEmbeddings()
    template_dict = {}
    source_dict = {}
    for i in range(0, len(template_read)):
        template_embed = embeddings.embed_query(str(template_read[i]).strip())
        template_dict[str(template_read[i]).strip()] = template_embed

    for j in range(0, len(source_read)):
        source_embed = embeddings.embed_query(str(source_read[j]).strip())
        source_dict[str(source_read[j]).strip()] = source_embed

    return template_dict, source_dict

def match_csv_file_columns_with_cosine_sim(template, source):
    column_mapping = template.replace(".txt", "") + "_" + source.replace(".txt", "") + "_cos_sim.csv"

    with open(column_mapping, 'w') as mapping_file:
        mapping_file.write("template;source;cos_sim" + "\n")
        template_read = get_column_names(template)
        source_read = get_column_names(source)
        template_dict, source_dict = find_embeddings(template_read, source_read)
        for keytd, td in template_dict.items():
            max_ratio = 0
            source_name_column = ""
            for keysd, sd in source_dict.items():
                cos_sim = cosine_similarity(td, sd)
                if cos_sim > max_ratio:
                    max_ratio = cos_sim
                    source_name_column = keysd

            print(keytd, source_name_column, max_ratio)
            mapping_file.write(keytd + ";" + source_name_column + ";" + str(max_ratio) + "\n")

    return column_mapping

def match_csv_file_columns_with_prompt(template, source):
    df_template = pd.read_csv(template)
    template_columns = df_template.columns.tolist()
    template_data = {}

    for i in range(0, len(template_columns)):
        data = df_template[template_columns[i]][0]
        template_data[template_columns[i]] = data

    df_source = pd.read_csv(source)
    source_columns = df_source.columns.tolist()
    source_data = {}
    for i in range(0, len(source_columns)):
        data = df_source[source_columns[i]][0]
        source_data[source_columns[i]] = data

    response = openai.ChatCompletion.create(
        model='gpt-4',
        messages=[{"role":"user", "content": f"given following column names from the template set {template_data} find most similar column names out of the source set {source_data} "
                                             f"in case of ambiguous mapping, choose the first option "
                                             f"find for chosen column names from the template set {template_data} values from {template_data.values()} by keys = column name "
                                             #TODO: return in python format
                                             f"and define the data format for the found values "
                                             f"find for chosen column names from the source set {source_data} values from {source_data.values()} by keys = column name "
                                             #TODO: return in python format
                                             f"and define the data format for the found values "
                                             f"struct json for chosen column names and data format with structure: template_column: name of tempate column, template_data_format: data format of template column, source_column: name of source column, source_data_format: data format of source column. "
                                             f"Don't make any hints, give me result as json only"}
        ]
        )
    content = response['choices'][0].message.content

    column_mapping = template.replace(".csv", "") + "_" + source.replace(".csv", "") + "_model.txt"
    with open(column_mapping, "w") as wfile:
        wfile.write(str(content).strip())

    return column_mapping

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--source')
    parser.add_argument('--template')
    parser.add_argument('--target')
    args = parser.parse_args()

    if len(args.source) > 0 & len(args.template):
        template_model = match_csv_file_columns_with_prompt(str(args.template).strip(), str(args.source).strip())
        map_columns(str(args.template).strip(), str(args.source).strip(), template_model, str(args.target).strip())
        #map_columns(str(args.template).strip(), str(args.source).strip(), "template_table_B_model.txt", str(args.target).strip())
        #ratio_file = match_csv_file_columns_with_levenstain_dist(str(args.template).strip(), str(args.source).strip())
        #match_csv_file_columns_with_cosine_sim(str(args.template).strip(), str(args.source).strip())
