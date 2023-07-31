# data-platform-api-quantity-unit-deletes-rmq-kube

data-platform-api-quantity-unit-deletes-rmq-kube は、周辺業務システム　を データ連携基盤 と統合することを目的に、API 数量単位データに削除フラグを設定するマイクロサービスです。  
https://xxx.xxx.io/api/API_QUANTITY_UNIT_SRV/deletes/

## 動作環境
data-platform-api-quantity-unit-deletes-rmq-kube の動作環境は、次の通りです。  
・ OS: LinuxOS （必須）  
・ CPU: ARM/AMD/Intel（いずれか必須）  

## 本レポジトリ が 対応する API サービス
data-platform-api-quantity-unit-deletes-rmq-kube が対応する APIサービス は、次のものです。

APIサービス URL: https://xxx.xxx.io/api/DPFM_API_QUANTITY_UNIT_SRV/deletes/

## API への 値入力条件 の 初期値
data-platform-api-quantity-unit-deletes-rmq-kube において、API への値入力条件の初期値は、入力ファイルレイアウトの種別毎に、次の通りとなっています。  

## データ連携基盤のAPIの選択的コール

Latona および AION の データ連携基盤 関連リソースでは、Inputs フォルダ下の sample.json の accepter に取得したいデータの種別（＝APIの種別）を入力し、指定することができます。  
なお、同 accepter にAll(もしくは空白)の値を入力することで、全データ（＝全APIの種別）をまとめて取得することができます。  

## Output  
本マイクロサービスでは、[golang-logging-library-for-data-platform](https://github.com/latonaio/golang-logging-library-for-data-platform) により、以下のようなデータがJSON形式で出力されます。  
以下の sample.json の例は 数量単位 の 数量単位データ に削除フラグが設定された結果の JSON の例です。  
以下の項目のうち、"XXXXXXXXXX" ～ "XXXXXXXXXX" は、/DPFM_API_Output_Formatter/type.go 内 の Type Header {} による出力結果です。"cursor" ～ "time"は、golang-logging-library による 定型フォーマットの出力結果です。  

```
XXXXXXXX
```
