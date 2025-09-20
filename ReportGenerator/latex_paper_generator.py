from paper_generator_interface import PaperGeneratorInterface
from sample_tester import generate_sample


class LatexPaperGenerator(PaperGeneratorInterface):
    def __init__(self):
        self.title = ""
        self.sub_title = ""
        self.abstract = ""

        self.contents = []

        self.chapters = []     # [(rank, chapter_title)]
        self.sentences = []    # [(rank, sentence)]
        self.images = []       # [(path, title)]
        self.tables = []       # [(data, title)]

        self.refs = []         # [(author, title, year)]
        self.authors = []      # [(author, title, year)]
        self.double_column = False

    def set_title(self, title: str):
        self.title = title

    def set_sub_title(self, sub_title: str):
        self.sub_title = sub_title

    def set_abstract(self, abstract: str):
        self.abstract = abstract

    def add_chapter(self, chapter: str, rank: int):
        if rank == 1:
            self.contents.append("\\section{{{}}}".format(chapter))
        elif rank == 2:
            self.contents.append("\\subsection{{{}}}".format(chapter))
        else:
            self.contents.append("\\paragraph{{{}}}".format(chapter))

    def add_sentence(self, sentence: str):
        self.contents.append(sentence)

    def add_image(self, path: str, title: str):
        self.contents.append("\\begin{figure}[h]")
        self.contents.append("\\centering")
        self.contents.append("\\includegraphics[width=0.8\\linewidth]{{{}}}".format(path))
        self.contents.append("\\caption{{{}}}".format(title))
        self.contents.append("\\end{figure}")

    def add_table(self, data: list, title: str):
        cols = len(data[0])
        self.contents.append("\\begin{table}[h]")
        self.contents.append("\\centering")
        self.contents.append("\\caption{{{}}}".format(title))
        self.contents.append("\\begin{tabular}{" + "l"*cols + "}")
        self.contents.append("\\toprule")
        for row in data:
            self.contents.append(" & ".join([str(d) for d in row]) + " \\\\")
        self.contents.append("\\bottomrule")
        self.contents.append("\\end{tabular}")
        self.contents.append("\\end{table}")


    def add_ref(self, author: str, title: str, year: int = None):
        self.refs.append((author, title, year))

    def add_author(self, author: str, title: str, year: int = None):
        self.authors.append((author, title, year))

    def set_double_column(self, mode: bool):
        self.double_column = mode

    def run(self, path: str):
        doc_class = "twocolumn" if self.double_column else "onecolumn"
        latex = []
        latex.append("\\documentclass[{}]{{article}}".format(doc_class))
        latex.append("\\usepackage[dvipdfmx]{graphicx}")
        latex.append("\\usepackage{booktabs}")
        latex.append("\\title{{{}}}".format(self.title))

        if self.sub_title:
            print("Warning: Sub title is not supperted in this version!")

        # 著者はまとめて一行に
        if self.authors:
            author_line = ", ".join([a for a,_,_ in self.authors])
            latex.append("\\author{{{}}}".format(author_line))
        else:
            latex.append("\\author{}")
        latex.append("\\date{}")
        latex.append("\\begin{document}")
        latex.append("\\maketitle")


        if self.abstract:
            latex.append("\\begin{abstract}")
            latex.append(self.abstract)
            latex.append("\\end{abstract}")

        for content in self.contents:
            latex.append(content)

        # 参考文献
        if self.refs:
            latex.append("\\begin{thebibliography}{99}")
            for author, title, year in self.refs:
                year_str = str(year) if year else ""
                latex.append("\\bibitem{{}} {}. \\textit{{{}}} {}".format(author, title, year_str))
            latex.append("\\end{thebibliography}")

        latex.append("\\end{document}")

        with open(path, 'w', encoding='utf-8') as f:
            f.write("\n".join(latex))

if __name__ == '__main__':
    path="sample_latex.tex"
    pg = LatexPaperGenerator()
    generate_sample(pg, path)
