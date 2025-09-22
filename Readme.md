# CityGML 解析レポート生成AI

## 環境構築

環境構築手順が若干複雑なため、Docker を使用しています。
Windows 環境の場合はWSL を利用してUbuntu を使用するようにしてください。

1. コンテナをビルドして起動する。
```bash
$ docker-compose build --no-cache
$ docker-compose up -d
```

2. コンテナに入る
```bash
$ docker-compose exec ai-reporter bash
```

3. 前処理を実行する
```bash
$ python3 install.py --help
$ python3 install.py (CityGML データのディレクトリ名)
```

4. レポートを生成する
```bash
$ python3 ai_report_generator.py (CityGML データのディレクトリ名)
```

5. 生成されたファイルを確認する

```bash
$ ls *.pdf
result.pdf
```

