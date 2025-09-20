# ReportGenerator

```py
    path="sample_paper_with_pagenum.pdf"
    pg = PaperGenerator()

    # 表題を設定
    pg.set_title('論文タイトル：PythonによるPDF論文自動生成')

    # 副題を設定（設定しない場合は出力されない）
    pg.set_sub_title('サブタイトル')

    # 要旨を設定
    abstract_text = (
        "ここに論文の概要(Abstract)を記載します。"
        "この部分は1段組みで小さな文字サイズです。"
        "ReportLabを用いてタイトルページから本文、引用文献まで自動生成する手法を示します。"
    )
    pg.set_abstract(abstract_text)

    # 章タイトルをアウトラインレベル（1～8）を指定して追加
    pg.add_chapter("チャプターC", 1)
    pg.add_chapter("チャプターCA", 2)
    pg.add_chapter("チャプターCAA", 3)

    # 本文を追加
    for i in range(10):
        pg.add_sentence(
            f"{i+1}段落目：これは2段組み本文のサンプルテキストです。"
            "論文本文として長い文章が続くことを想定しています。"
            "----------------------------------------------"
            "----------------------------------------------"
            "----------------------------------------------")


    # 参考文献を追加
    pg.add_ref("S. Ogata.", "Automatic Paper Generation with Python", 2025)

    # 著者を追加
    pg.add_author('azarashin', 'pit-creation')

    # 2段組みにする
    pg.set_double_column(True)

    pg.run(path)

```