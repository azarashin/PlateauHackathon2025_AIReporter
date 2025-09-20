import sys
import json
import os

sys.path.append('./AIAgentForCityGML')
from AIAgentForCityGML.agent_manager import AgentManager

sys.path.append('./ReportGenenrator')
from ReportGenenrator.paper_generator import PaperGenerator

def get_prompt() -> str:
    purpose_list = []
    while True:
        purpose = input("レポートの目的を入力してください(空行で終了):")
        if purpose.strip() == '':
            break
        purpose_list.append(purpose)
        
    template = open('prompt_tepmlate.txt', 'r', encoding='utf-8').read()
    purpose_string = '\n'.join([f'- {d}' for d in purpose_list])
    prompt = template.replace('{{PURPOSE_LIST}}', purpose_string)
    return prompt

def convert_attributed_table(source):
    attribs = []
    ret = []
    for key in source[0]:
        attribs.append(key)
    ret.append(attribs)
    for line in source[1:]:
        ret.append([line[key] for key in attribs])
    return ret
        
        

def generate_report(response: str, output_path: str):
    pg = PaperGenerator('ShipporiMincho', './ReportGenenrator/Shippori_Mincho/ShipporiMincho-Regular.ttf')
    abstract_text = (
        "ここに論文の概要(Abstract)を記載します。"
        "この部分は1段組みで小さな文字サイズです。"
        "ReportLabを用いてタイトルページから本文、引用文献まで自動生成する手法を示します。"
    )
    pg.set_title('論文タイトル：PythonによるPDF論文自動生成')
    pg.set_abstract(abstract_text)

        
    json_data = json.loads(response)
    for section in json_data:
        if not "title" in section:
            continue
        if not "content" in section:
            continue
        pg.add_chapter(section["title"], 0)
        for content in section["content"]:
            if not "type" in content:
                continue
            if not "content" in content:
                continue
            if content["type"] == "text":
                pg.add_sentence(content["content"])
            if content["type"] == "image" and "title" in content:
                path = content["content"]
                if os.path.isfile(path):
                    pg.add_image(content["content"], content["title"])
            if content["type"] == "table" and "title" in content:
                pg.add_table(convert_attributed_table(content["content"]), content["title"])
    pg.run(output_path)

def test_create_pdf_from_dummy_data():
    response = '[{"title": "東京駅周辺の建物分布","content": [ \
            {"type": "text", "content": "東京は日本の首都であり、多くの建物が密集しています。特に、ビジネスエリアや商業施設が多く存在し、様々な用途の建物が見られます。"}, \
            {"type": "table", "content": [ \
                {"建物の種類": "オフィスビル", "棟数": 40}, \
                {"建物の種類": "商業施設", "棟数": 30}, \
                {"建物の種類": "住宅", "棟数": 30} \
            ], "title": "建物の内訳"} \
    ]}]'
    print(response)
    generate_report(response, 'result.pdf')


if __name__ == '__main__':
    prompt = get_prompt()
    print(prompt)

    agentManager = AgentManager()
    response = agentManager.query(prompt)
    print(response)
    print(type(response))
    generate_report(response, 'result.pdf')


