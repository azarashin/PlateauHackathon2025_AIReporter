class PaperGeneratorInterface(object):
    def set_title(self, title: str):
        raise Exception

    def set_sub_title(self, sub_title: str):
        raise Exception

    def set_abstract(self, abstract: str):
        raise Exception

    def add_chapter(self, chapter: str, rank: int):
        raise Exception

    def add_sentence(self, sentence: str):
        raise Exception

    def add_image(self, path: str, title: str):
        raise Exception

    def add_table(self, data: list, title: str):
        raise Exception

    def add_ref(self, author: str, title: str, year: int = None):
        raise Exception

    def add_author(self, author: str, title: str, year: int = None):
        raise Exception

    def set_double_column(self, mode: bool):
        raise Exception

    def run(self, path: str):
        raise Exception
