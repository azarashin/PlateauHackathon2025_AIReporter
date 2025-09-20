from reportlab.platypus import (
    BaseDocTemplate, PageTemplate, Frame, Paragraph, Spacer,
    NextPageTemplate, PageBreak, FrameBreak, Image, TableStyle, Table
)
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.lib.enums import TA_CENTER
from reportlab.lib import colors
from reportlab.platypus import LongTable
from decimal import Decimal, InvalidOperation

from paper_generator_interface import PaperGeneratorInterface
from sample_tester import generate_sample

class Author:
    def __init__(self, name: str, organization: str):
        self.name = name
        self.organization = organization

    def __str__(self):
        return f'{self.name}, {self.organization}'

class Reference:
    def __init__(self, author: str, title: str, year: int = None):
        self.author = author
        self.title = title
        self.year = year

    def __str__(self):
        if self.year == None:
            return f'{self.author}, {self.title}.'
        return f'{self.author}, {self.title}, {self.year}.'

class MyDocTemplate(BaseDocTemplate):

    def __init__(self, filename, **kw):
        self.allowSplitting = 0
        BaseDocTemplate.__init__(self, filename, **kw)
#        template = PageTemplate('normal', [Frame(2.5*cm, 2.5*cm, 15*cm, 25*cm, id='F1')])
#        self.addPageTemplates(template)

    def afterFlowable(self, flowable):
        "Registers TOC entries."
        if flowable.__class__.__name__ == 'Paragraph':
            text = flowable.getPlainText()
            style = flowable.style.name
            if style == 'CapterRank1':
                self.notify('TOCEntry', (0, text, self.page))
            if style == 'CapterRank2':
                self.notify('TOCEntry', (1, text, self.page))
            if style == 'CapterRank3':
                self.notify('TOCEntry', (2, text, self.page))
            if style == 'CapterRank4':
                self.notify('TOCEntry', (3, text, self.page))
            if style == 'CapterRank5':
                self.notify('TOCEntry', (4, text, self.page))
            if style == 'CapterRank6':
                self.notify('TOCEntry', (5, text, self.page))
            if style == 'CapterRank7':
                self.notify('TOCEntry', (6, text, self.page))
            if style == 'CapterRank8':
                self.notify('TOCEntry', (7, text, self.page))

class PaperGenerator(PaperGeneratorInterface):
    def __init__(self, font='HeiseiMin-W3', path_to_font=None):
        self._font = font
        self._title = 'NO TITLE...'
        self._sub_title = None
        self._authors = []
        self._abstract = 'no abstract...'
        self._contents = []
        self._refs = []
        self._images = {}
        self._tables = {}
        self._double_colmuns = False
        self._chapter_numbers = [0, 0, 0, 0, 0, 0, 0, 0]
        self._body_style = ParagraphStyle(
            'Body', fontName=self._font, fontSize=10.5, leading=11, spaceAfter=5
        )

        self._image_description_style = ParagraphStyle(
            'ImageDescription', fontName=self._font, fontSize=10.5, leading=11, spaceAfter=25, alignment=1, 
        )

        self._table_description_style = ParagraphStyle(
            'TableDescription', fontName=self._font, fontSize=10.5, leading=11, spaceAfter=25, alignment=1, 
        )
        self._table_number_style = ParagraphStyle(
            'Body', fontName=self._font, fontSize=9, leading=11, spaceAfter=5, alignment=2
        )
        self._table_string_style = ParagraphStyle(
            'Body', fontName=self._font, fontSize=9, leading=11, spaceAfter=5, alignment=0
        )

        self._chapter_styles = [
            ParagraphStyle('CapterRank1', fontName=self._font, fontSize=15, leading=17, spaceBefore=10, spaceAfter=5, leftIndent=10, outlineLevel=0), 
            ParagraphStyle('CapterRank2', fontName=self._font, fontSize=14, leading=16, spaceBefore=9, spaceAfter=5, leftIndent=10, outlineLevel=1), 
            ParagraphStyle('CapterRank3', fontName=self._font, fontSize=13, leading=15, spaceBefore=8, spaceAfter=5, leftIndent=10, outlineLevel=2), 
            ParagraphStyle('CapterRank4', fontName=self._font, fontSize=12, leading=14, spaceBefore=7, spaceAfter=5, leftIndent=10, outlineLevel=3), 
            ParagraphStyle('CapterRank5', fontName=self._font, fontSize=11, leading=13, spaceBefore=6, spaceAfter=5, leftIndent=10, outlineLevel=4), 
            ParagraphStyle('CapterRank6', fontName=self._font, fontSize=11, leading=13, spaceBefore=6, spaceAfter=5, leftIndent=10, outlineLevel=5), 
            ParagraphStyle('CapterRank7', fontName=self._font, fontSize=11, leading=13, spaceBefore=6, spaceAfter=5, leftIndent=10, outlineLevel=6), 
            ParagraphStyle('CapterRank8', fontName=self._font, fontSize=11, leading=13, spaceBefore=6, spaceAfter=5, leftIndent=10, outlineLevel=7), 
        ]

        if path_to_font:
            pdfmetrics.registerFont(TTFont(self._font, path_to_font))
        else:
            pdfmetrics.registerFont(UnicodeCIDFont(self._font))
        



    def set_double_column(self, mode: bool):
        self._double_colmuns = mode

    def set_title(self, title: str):
        self._title = title 

    def set_sub_title(self, sub_title : str):
        self._sub_title = sub_title 

    def add_author(self, name: str, organization: str):
        self._authors.append(Author(name, organization))

    def set_abstract(self, abstract: str):
        self._abstract = abstract

    def add_table(self, data: list, title: str):
        if len(data) == 0:
            return
        # Table オブジェクト生成
        data = self._transpose(data)
        for line in data:
            quant = self._get_quantize(line[1:])
            line[0] = self._get_table_value(line[0], 0)
            line[1:] = [self._get_table_value(d, quant) for d in line[1:]]
        data = self._transpose(data)

        table = LongTable(data)  # 列幅をpt単位で指定（省略可）

        # スタイル設定
        table.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),   # 1行目背景色
            ('TEXTCOLOR',  (0,0), (-1,0), colors.black),       # 1行目文字色
            ('ALIGN',      (0,0), (-1,-1), 'CENTER'),          # 全セル中央寄せ
            ('GRID',       (0,0), (-1,-1), 0.5, colors.grey),  # 枠線
            ('FONTNAME',   (0,0), (-1,0), self._font),   # 1行目フォント太字
            ('BOTTOMPADDING', (0,0), (-1,0), 8),               # 1行目下余白
        ]))

        index = len(self._tables) + 1
        self._contents.append(Paragraph(f'表{index}. {title}', self._table_description_style))
        self._contents.append(table)
        if title in self._tables:
            print(f'{title} is already registed.')
        else:
            self._tables[title] = index

        

    def _get_table_value(self, value: any, quant: int):
        if type(value) is int and quant >= 1:
            return Paragraph(str(value), self._table_number_style)
        if type(value) is float or type(value) is int:
            return Paragraph(f"{Decimal(value).quantize(quant):f}", self._table_number_style)
        return Paragraph(value, self._table_string_style)

    def add_image(self, path: str, title: str):
        # 画像を挿入
        img = Image(path, width=200, height=150)   # 幅・高さをpt単位で指定
        self._contents.append(img)

        # 画像の下にテキスト
        index = len(self._images) + 1
        self._contents.append(Spacer(1, 12))        
        self._contents.append(Paragraph(f'図 {index}. {title}', self._image_description_style))
        if title in self._images:
            print(f'{title} is already registed.')
        else:
            self._images[title] = index
            

    def add_sentence(self, sentence: str):
        self._contents.append(Paragraph(sentence, self._body_style))

    def add_chapter(self, chapter: str, chapter_rank = 0):
        self._chapter_numbers[chapter_rank] += 1
        title = f'{self._get_chapter_number(chapter_rank)}. {chapter}'
        self._contents.append(Paragraph(title, self._chapter_styles[chapter_rank]))
        for i in range(chapter_rank+1, len(self._chapter_numbers)):
            self._chapter_numbers[i] = 0
    
    def _get_chapter_number(self, chapter_rank):
        return '.'.join([str(d) for d in self._chapter_numbers[:chapter_rank + 1]])
        

    def add_ref(self, author: str, title: str, year: int = None):
        self._refs.append(Reference(author, title, year))

    # --- ページ番号を描画する関数 ---
    def _add_page_number(self, canvas, doc):
        """
        各ページ下部中央にページ番号を描画
        """
        page_num = canvas.getPageNumber()
        text = f"Page {page_num}"
        canvas.setFont(self._font, 9)
        # 下部中央に配置
        canvas.drawCentredString(A4[0] / 2.0, 15, text)

    def run(self, path: str):

        doc = MyDocTemplate(path, pagesize=A4)

        doc = self._setup_template(doc)

        story = []

        story = self._add_title(story)

        story.append(NextPageTemplate('TableOfContents'))
        story.append(PageBreak())
        story = self._add_table_of_contents(story)

        if self._double_colmuns:
            story.append(NextPageTemplate('BodyPagesInDoubleColumn'))
        else:
            story.append(NextPageTemplate('BodyPagesInSingleColumn'))

        story.append(PageBreak())

        story = self._add_body(story)

        story.append(NextPageTemplate('References'))
        story.append(PageBreak())

        story = self._add_reference(story)

        doc.multiBuild(story)

    def _add_table_of_contents(self, story):
        style_toc = ParagraphStyle(name='TOCTitle', fontName=self._font,
                          fontSize=18, alignment=TA_CENTER, spaceAfter=20)

        story.append(Paragraph("目次", style_toc))
        toc = TableOfContents()
        # TOCのスタイルをカスタマイズ
        toc.levelStyles = [
            ParagraphStyle('toc_level1', fontName=self._font, fontSize=12, leftIndent=20, firstLineIndent=-20, spaceBefore=5),
            ParagraphStyle('toc_level2', fontName=self._font, fontSize=10, leftIndent=40, firstLineIndent=-20, spaceBefore=2),
            ParagraphStyle('toc_level3', fontName=self._font, fontSize=10, leftIndent=50, firstLineIndent=-20, spaceBefore=2),
            ParagraphStyle('toc_level4', fontName=self._font, fontSize=10, leftIndent=60, firstLineIndent=-20, spaceBefore=2),
            ParagraphStyle('toc_level5', fontName=self._font, fontSize=10, leftIndent=70, firstLineIndent=-20, spaceBefore=2),
            ParagraphStyle('toc_level6', fontName=self._font, fontSize=10, leftIndent=80, firstLineIndent=-20, spaceBefore=2),
            ParagraphStyle('toc_level7', fontName=self._font, fontSize=10, leftIndent=90, firstLineIndent=-20, spaceBefore=2),
            ParagraphStyle('toc_level8', fontName=self._font, fontSize=10, leftIndent=100, firstLineIndent=-20, spaceBefore=2),
        ]

        story.append(toc)
        return story

    def _add_reference(self, story):
        reference_style = ParagraphStyle(
            'Reference', fontName=self._font, fontSize=9, leading=11
        )

        # 最後のページ：引用文献
        if len(self._refs) > 0:
            for i in range(len(self._refs)):
                story.append(Paragraph(f'{i + 1}. {self._refs[i]}', reference_style))
        return story

    def _add_body(self, story):

        # 本文（1ページ目下部2段組から開始）
        for content in self._contents:
            story.append(content)
        return story

    def _setup_template(self, doc):
        page_width, page_height = A4
        margin = 40
        gap = 20

        # -----------------------
        # 表紙
        # -----------------------
        top_ypos = A4[1] * 0.2
        top_height = A4[1] * 0.3
        frame_title = Frame(
            margin,
            page_height - margin - top_height - top_ypos,
            page_width - 2*margin,
            top_height,
            id='top'
        )
        abstract_ypos = 100
        abstract_height = 200
        frame_abstract = Frame(margin, abstract_ypos, page_width - 2*margin, abstract_height, id='abstract')
        template_title = PageTemplate(id='FirstPage',
                                    frames=[frame_title, frame_abstract])



        # -----------------------
        # 目次
        # -----------------------
        frame_width_in_toc = (page_width - 2*margin - gap)
        frame_all_in_toc = Frame(margin, margin, frame_width_in_toc, page_height - 2*margin, id='table_of_contents')
        template_body_in_toc = PageTemplate(id='TableOfContents',
                                    frames=[frame_all_in_toc],
                                    onPage=self._add_page_number)   # ★ ページ番号追加

        # -----------------------
        # 2ページ目以降：1カラム本文
        # -----------------------
        frame_width_in_single = (page_width - 2*margin - gap)
        frame_all_in_single = Frame(margin, margin, frame_width_in_single, page_height - 2*margin, id='all_in_single')
        template_body_in_single = PageTemplate(id='BodyPagesInSingleColumn',
                                    frames=[frame_all_in_single],
                                    onPage=self._add_page_number)   # ★ ページ番号追加

        # -----------------------
        # 2ページ目以降：2カラム本文
        # -----------------------
        frame_width_in_double = (page_width - 2*margin - gap) / 2
        frame_left_all_in_double = Frame(margin, margin, frame_width_in_double, page_height - 2*margin, id='left_all')
        frame_right_all_in_double = Frame(margin + frame_width_in_double + gap, margin, frame_width_in_double, page_height - 2*margin, id='right_all')
        template_body_in_double = PageTemplate(id='BodyPagesInDoubleColumn',
                                    frames=[frame_left_all_in_double, frame_right_all_in_double],
                                    onPage=self._add_page_number)   # ★ ページ番号追加

        # -----------------------
        # 最終ページ：引用文献
        # -----------------------
        frame_refs = Frame(margin, margin, page_width - 2*margin, page_height - 2*margin, id='refs')
        template_refs = PageTemplate(id='References',
                                    frames=[frame_refs],
                                    onPage=self._add_page_number)   # ★ ページ番号追加

        doc.addPageTemplates([template_title, template_body_in_toc, template_body_in_single, template_body_in_double, template_refs])
        return doc


    def _add_title(self, story):
        # タイトル
        # --- スタイル定義 ---
        title_style = ParagraphStyle(
            'Title', fontName=self._font, fontSize=24, leading=28, alignment=1, spaceAfter=20
        )
        sub_title_style = ParagraphStyle(
            'SubTitle', fontName=self._font, fontSize=20, leading=28, alignment=1, spaceAfter=20
        )

        authors_style = ParagraphStyle(
            'Authors', fontName=self._font, fontSize=12, leading=14, alignment=1, spaceAfter=5
        )


        abstract_title_style = ParagraphStyle(
            'Abstract', fontName=self._font, fontSize=14, leading=16, spaceAfter=20, alignment=1, leftIndent=80, rightIndent=80
        )
        abstract_body_style = ParagraphStyle(
            'Abstract', fontName=self._font, fontSize=10, leading=12, alignment=1, leftIndent=80, rightIndent=80
        )

        story.append(NextPageTemplate('FirstPage'))

        story.append(Paragraph(self._title, title_style))
        if self._sub_title:
            story.append(Paragraph(f' - {self._sub_title} - ', sub_title_style))

        for author in self._authors:
            story.append(Paragraph(str(author), authors_style))

        story.append(FrameBreak())
        
        story.append(Paragraph('要旨', abstract_title_style))
        story.append(Paragraph(self._abstract, abstract_body_style))
        return story

    def _transpose(self, matrix):
        """
        2次元リスト matrix の行と列を入れ替えた新しいリストを返す
        """
        # zip(*matrix) はタプルを返すので list に変換
        return [list(row) for row in zip(*matrix)]

    '''
    量子化(quantize)する単位を調べる。
    例：
    [12, 2.3, 0.03] => 0.01
    '''
    def _get_quantize(self, values, digits=None):
        """
        values: 数値 or 文字列のリスト（例: [1, 2.3, '4.567']）
        digits: そろえる小数桁数。Noneなら入力の中で最大の小数桁数に自動合わせ

        戻り値: 小数部の桁数をそろえた文字列リスト
        """
        decs = []
        max_frac = 0

        # 1) Decimal化 & 最大小数桁数の決定
        for v in values:
            # 文字列化してからDecimalに渡すと見た目どおりの桁が保てます
            s = str(v)
            try:
                d = Decimal(s)
            except InvalidOperation:
                # NaN/INFなどや数値でない場合はそのまま返すために保持
                decs.append(s)
                continue

            decs.append(d)
            if d.is_finite():
                # 小数桁数 = 負のexponentの絶対値
                frac = -d.as_tuple().exponent if d.as_tuple().exponent < 0 else 0
                if frac > max_frac:
                    max_frac = frac

        # 2) そろえる桁数を決定
        scale = max_frac if digits is None else int(digits)
        quant = Decimal(1).scaleb(-scale)  # 10**(-scale)
        return quant

if __name__ == '__main__':
    path="sample_paper_with_pagenum.pdf"
    pg = PaperGenerator('ShipporiMincho', './Shippori_Mincho/ShipporiMincho-Regular.ttf')
    generate_sample(pg, path)
