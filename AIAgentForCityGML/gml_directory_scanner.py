import os

from pathlib import Path

'''
CityGML データの入ったディレクトリのリストを取得する。
'''
class GMLDirectoryScanner:
    def __init__(self, root_dir):
        self.root_dir = root_dir
        self.required_subdirs = [
            "codelists",
            "metadata",
            "schemas",
            "specification",
            "udx",
            os.path.join("udx", "bldg") # 最低限ビル情報だけは含むように
        ]

    def _is_valid_directory(self, path):
        """
        指定されたディレクトリに必要なサブディレクトリがすべて存在するかを確認
        """
        for subdir in self.required_subdirs:
            full_path = os.path.join(path, subdir)
            if not os.path.isdir(full_path):
                return False
        return True

    def find_valid_directories(self):
        """
        DDD 以下を再帰的に探索し、条件を満たすディレクトリの一覧を返す
        """
        valid_dirs = []
        for dirpath, dirnames, filenames in os.walk(self.root_dir):
            # 各ディレクトリに対して条件チェック
            if self._is_valid_directory(dirpath):
                valid_dirs.append(Path(dirpath))
        return valid_dirs

if __name__ == "__main__":
    scanner = GMLDirectoryScanner("CityGMLData")
    results = scanner.find_valid_directories()
    for path in results:
        print(path)
