import os
import openai
import argparse
import pandas as pd
from fuzzywuzzy import fuzz
from langchain.embeddings import OpenAIEmbeddings
from openai.embeddings_utils import cosine_similarity
import json
import numpy

os.environ["OPENAI_API_KEY"] = "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"
openai.api_key = os.getenv("OPENAI_API_KEY")

def info_columns(table):
    table = table + ".csv"
    df = pd.read_csv(table)
    df_columns = df.columns.tolist()
    name_file = 'columns_' + table.replace(".csv", ".txt")
    with open(name_file, 'w') as wfile:
        for i in range(0, len(df_columns)):
            wfile.write(str(df_columns[i]) + "\n")
    return name_file

def find_in_dict(data, name_column):
    for key_value, value_value in data.items():
        if str(name_column).strip() == str(value_value).strip():
            return True
    return False

def mapping_columns(template, source, mapping_file, target):
    template = template + ".csv"
    source = source + ".csv"
    target = target + ".csv"

    df_template = pd.read_csv(template)
    template_columns = df_template.columns.tolist()

    df_source = pd.read_csv(source)

    template_dict_list = []

    with open(mapping_file) as json_file:
        data = json.load(json_file)
        for i in range(0, len(template_columns)):
            if type(data) == dict:
                for key, value in data.items():
                    if type(value) == list:
                        for j in range(0, len(value)):
                            if type(value[j]) == dict:
                                if find_in_dict(value[j], template_columns[i]):
                                    template_dict_list.append(value[j])
            if type(data) == list:
                for j in range(0, len(data)):
                    if type(data[j]) == dict:
                        if find_in_dict(data[j], template_columns[i]):
                            template_dict_list.append(data[j])

    df_source_need = pd.DataFrame()
    for i in range(0, len(template_dict_list)):
        df_source_need[[template_dict_list[i]['template_column']]] = df_source[[template_dict_list[i]['source_column']]]
        if (str(template_dict_list[i]['template_data_format']).strip().lower() != str(template_dict_list[i]['source_data_format']).strip().lower()):
            try:
                df_source_need[[template_dict_list[i]['template_column']]].astype(str(template_dict_list[i]['template_data_format']).lower())
            except:
                template_format = str(template_dict_list[i]['template_data_format']).lower()
                source_format = str(template_dict_list[i]['source_data_format']).lower()
                print(f'no format {template_format} or {source_format}')

    df_source_need.to_csv(target)

def find_similar_fuzz(template, source):
    template = template + ".csv"
    source = source + ".csv"

    writename = template.replace(".txt", "") + "_" + source.replace(".txt", "") + "_ratio.csv"

    with open(template, 'r') as templateread, open(source, 'r') as sourceread, open(writename, 'w') as writefile:
        writefile.write("template;source;ratio"+"\n")
        template_read = templateread.readlines()
        source_read = sourceread.readlines()
        for i in range(0, len(template_read)):
            max_ratio = 0
            source_name_column = ""
            for j in range(0, len(source_read)):
                current_ratio = fuzz.ratio(template_read[i], source_read[j])
                if current_ratio > max_ratio:
                    max_ratio = current_ratio
                    source_name_column = source_read[j]
            print(template_read[i], source_name_column, max_ratio)
            writefile.write(str(template_read[i]).strip() + ";" + str(source_name_column).strip() + ";" + str(max_ratio) + "\n")

    return writename

def find_similar_openAI(template, source):
    template = template + ".csv"
    source = source + ".csv"
    embeddings = OpenAIEmbeddings()
    writename = template.replace(".txt", "") + "_" + source.replace(".txt", "") + "_cos_sim.csv"

    with open(template, 'r') as templateread, open(source, 'r') as sourceread, open(writename, 'w') as writefile:
        writefile.write("template;source;cos_sim" + "\n")
        template_read = templateread.readlines()
        source_read = sourceread.readlines()
        template_dict = {}
        source_dict = {}
        for i in range(0, len(template_read)):
            template_embed = embeddings.embed_query(str(template_read[i]).strip())
            template_dict[str(template_read[i]).strip()] = template_embed

        for j in range(0, len(source_read)):
            source_embed = embeddings.embed_query(str(source_read[j]).strip())
            source_dict[str(source_read[j]).strip()] = source_embed

        for keytd, td in template_dict.items():
            max_ratio = 0
            source_name_column = ""
            for keysd, sd in source_dict.items():
                cos_sim = cosine_similarity(td, sd)
                if cos_sim > max_ratio:
                    max_ratio = cos_sim
                    source_name_column = keysd

            print(keytd, source_name_column, max_ratio)
            writefile.write(keytd+ ";" + source_name_column + ";" + str(max_ratio) + "\n")

    return writename

def ask_agent(template, source):
    template = template + ".csv"
    source = source + ".csv"

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
                                             f"and define the data format for the found values "
                                             f"find for chosen column names from the source set {source_data} values from {source_data.values()} by keys = column name "
                                             f"and define the data format for the found values "
                                             f"struct json for chosen column names and data format with structure: template_column: name of tempate column, template_data_format: data format of template column, source_column: name of source column, source_data_format: data format of source column. "
                                             f"Don't make any hints, give me result as json only"}
        ]
        )
    content = response['choices'][0].message.content

    writename = template.replace(".csv", "") + "_" + source.replace(".csv", "") + "_model.txt"
    with open(writename, "w") as wfile:
        wfile.write(str(content).strip())

    return writename
    #print(str(content).strip())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--source')
    parser.add_argument('--template')
    parser.add_argument('--target')
    args = parser.parse_args()

    if len(args.source) > 0 & len(args.template):
        #template_model = ask_agent(str(args.template).strip(), str(args.source).strip())
        #mapping_columns(str(args.template).strip(), str(args.source).strip(), template_model, str(args.target).strip())
        mapping_columns(str(args.template).strip(), str(args.source).strip(), "template_table_A_model.txt", str(args.target).strip())
        #template_file = info_columns(str(args.template).strip())
        #source_file = info_columns(str(args.source).strip())
        #ratio_file = find_similar_fuzz(template_file, source_file)
        #find_similar_openAI(template_file, source_file)