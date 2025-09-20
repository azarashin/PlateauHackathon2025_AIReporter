from paper_generator_interface import PaperGeneratorInterface


def generate_sample(pg: PaperGeneratorInterface, path: str):
        # Abstract
    abstract_text = (
        "ここに論文の概要(Abstract)を記載します。"
        "この部分は1段組みで小さな文字サイズです。"
        "ReportLabを用いてタイトルページから本文、引用文献まで自動生成する手法を示します。"
    )
    pg.set_title('論文タイトル：PythonによるPDF論文自動生成')
    pg.set_sub_title('サブタイトル')
    pg.set_abstract(abstract_text)
    pg.add_chapter("チャプターA", 0)
    for i in range(10):
        pg.add_sentence(
            f"{i+1}段落目：これは2段組み本文のサンプルテキストです。"
            "論文本文として長い文章が続くことを想定しています。"
            "----------------------------------------------"
            "----------------------------------------------"
            "----------------------------------------------")

    pg.add_chapter("チャプターB", 0)
    pg.add_chapter("チャプターBA", 1)
    for i in range(10):
        pg.add_sentence(
            f"{i+1}段落目：これは2段組み本文のサンプルテキストです。"
            "論文本文として長い文章が続くことを想定しています。"
            "----------------------------------------------"
            "----------------------------------------------"
            "----------------------------------------------")

    pg.add_chapter("チャプターBB", 1)
    for i in range(10):
        pg.add_sentence(
            f"{i+1}段落目：これは2段組み本文のサンプルテキストです。"
            "論文本文として長い文章が続くことを想定しています。"
            "----------------------------------------------"
            "----------------------------------------------"
            "----------------------------------------------")

    pg.add_chapter("チャプターBBA", 2)
    for i in range(10):
        pg.add_sentence(
            f"{i+1}段落目：これは2段組み本文のサンプルテキストです。"
            "論文本文として長い文章が続くことを想定しています。"
            "----------------------------------------------"
            "----------------------------------------------"
            "----------------------------------------------")

    pg.add_chapter("チャプターBC", 2)
    pg.add_chapter("チャプターBCA", 3)
    pg.add_chapter("チャプターBCAA", 4)
    pg.add_chapter("チャプターBCAAA", 5)
    pg.add_chapter("チャプターBCAAAA", 6)
    for i in range(10):
        pg.add_sentence(
            f"{i+1}段落目：これは2段組み本文のサンプルテキストです。"
            "論文本文として長い文章が続くことを想定しています。"
            "----------------------------------------------"
            "----------------------------------------------"
            "----------------------------------------------")

    pg.add_chapter("チャプターC", 1)
    pg.add_chapter("チャプターCA", 2)
    pg.add_chapter("チャプターCAA", 3)
    pg.add_chapter("チャプターCB", 2)
    pg.add_chapter("チャプターCBA", 3)
    pg.add_chapter("チャプターCBAA", 4)
    pg.add_image('./image/sample.jpg', 'サンプル画像')
    pg.add_table([
        ["実数", "整数", "文字列"],
        [12, 100, "説明A"],
        [1.1, 200, "説明B"],
        [0.02, 300, "説明C"]
    ], '表サンプル')


    pg.add_ref("Smith J.", "ReportLab Documentation", 2023)
    pg.add_ref("Ogata S.", "Automatic Paper Generation with Python", 2025)
    pg.add_ref("Example Author", "Sample References in ReportLab", 2024)

    pg.add_author('azarashin', 'pit-creation')
    pg.add_author('azarashinX', 'pnc')

    pg.set_double_column(True)

    pg.run(path)


