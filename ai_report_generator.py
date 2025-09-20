import sys
import json
import os
from pathlib import Path

sys.path.append('./AIAgentForCityGML')
from AIAgentForCityGML.agent_manager import AgentManager

sys.path.append('./ReportGenenrator')
from ReportGenenrator.paper_generator import PaperGenerator

class Author:
    def __init__(self, json):
        print(json)
        self.name = json["name"]
        self.organization = json["organization"]
    
    def __repr__(self) -> str:
        return (f"Author(name={self.name!r}, "
                f"organization={self.organization!r})")

class ReportConfig:
    def __init__(self):
        # JSONファイルから読み込む場合
        with open("report_config.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            self.title = data["title"]
            self.sub_title = data["sub-title"]
            self.authors = [Author(d) for d in data["authors"]]

    def __repr__(self) -> str:
        return (f"ReportInfo(title={self.title!r}, "
                f"sub_title={self.sub_title!r}, "
                f"authors={self.authors!r})")


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
    print(f'source: {source}')
    for key in source[0]:
        attribs.append(key)
    ret.append(attribs)
    for line in source[1:]:
        ret.append([line[key] for key in attribs])
    return ret
        
        

def generate_report(response: str, output_path: str):
    pg = PaperGenerator('ShipporiMincho', './ReportGenenrator/Shippori_Mincho/ShipporiMincho-Regular.ttf')
    config = ReportConfig()
    pg.set_title(config.title)
    pg.set_sub_title(config.sub_title)
    for author in config.authors:
        pg.add_author(author.name, author.organization)

        
    json_data = json.loads(response)


    pg.set_abstract(json_data["abstract"])
    for section in json_data["sections"]:
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
    if len(sys.argv) != 2:
        print("使い方: python extract_gml_features_all.py <ディレクトリパス>")
        sys.exit(1)

    base_dir = Path(sys.argv[1])
    if not base_dir.is_dir():
        print(f"ディレクトリが見つかりません: {base_dir}")
        sys.exit(1)
   
    prompt = get_prompt()
    print(prompt)

    agentManager = AgentManager([base_dir])
    response = agentManager.query(prompt)
    print('--- start ---')
    print(response)
    print('--- end ---')
    print(type(response))
    generate_report(response, 'result.pdf')


