# CityGML 解析レポート生成AI

## 環境構築

環境構築手順が若干複雑なため、Docker を使用しています。
Windows 環境の場合はWSL を利用してUbuntu を使用するようにしてください。

1. コンテナをビルドして起動する。

コンテナが起動した時点でFlask Webアプリも起動します。
```bash
$ docker-compose build --no-cache
$ docker-compose up -d
```

2. コンテナに入る
```bash
$ docker-compose exec ai-reporter bash
```

3.A. 前処理を実行する(CityGML データのディレクトリ名を指定する場合)

```bash
$ python3 -m install --help
$ python3 -m install (CityGML データのディレクトリ名)
```

3.B. 前処理を実行する(CityGML データのディレクトリをCityGMLData 配下から自動検出する場合)

```bash
$ python3 -m install --help
$ python3 -m install
```

4.A.1. レポートを生成する
```bash
$ python3 -m ai_report_generator
```

4.A.2. 生成されたファイルを確認する

```bash
$ ls *.pdf
result.pdf
```
4.B.1. ホスト側でブラウザにアクセスする

ブラウザから

http://localhost:5000

にアクセスしてください。

5. コンテナを抜ける

```bash
exit
```

6. コンテナを終了する

```bash
docker-compose down
```
