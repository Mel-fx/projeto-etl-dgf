
import os
import pandas as pan
import numpy as np

# Caminhos das camadas de dados (montados como volumes no docker-compose.yaml)
RAW_DIR = '/opt/airflow/data/raw'
BRONZE_DIR = '/opt/airflow/data/bronze'
SILVER_DIR = '/opt/airflow/data/silver'

# Colunas de cada domínio (nomes já traduzidos, após o rename feito no transform)
COLUNAS_PESSOAIS = ["Id_do_Cliente", "Nome", "Idade", "SSN", "Profissao"]

COLUNAS_BANCARIAS = [
    "Id_do_Cliente", "ID", "Mes", "Renda_Anual", "Salario_Liquido_Mensal",
    "Saldo_Mensal", "Taxa_de_Juros", "Numero_de_Emprestimos", "Tipo_de_Emprestimo",
    "Atraso_na_Data_de_Vencimento", "Numero_de_Pagamentos_Atrasados",
    "Quantidade_de_Mudancas_de_Limite", "Numero_de_Consultas_de_Credito",
    "Tipos_de_Creditos", "Saldo_Devedor", "Taxa_de_Utilizacao_de_Credito",
    "Tempo_de_Uso_de_Credito_em_Meses", "Pagamento_do_Valor_Minimo",
    "Valor_Total_em_Parcelas_Mensais", "Valor_Total_Investido_Mensalmente",
    "Comportamento_de_Pagamento", "Pontuacao_de_Credito", "Numero_de_Contas_Bancarias",
    "Numero_de_Cartoes_de_Credito"
]

# Nome do dataset no Kaggle
KAGGLE_DATASET = "parisrohan/credit-score-classification"

# Nome do bucket S3 
S3_BUCKET = "dgf-projeto-mel-172201861469-sa-east-1-an"


def extract():
    # Baixa o dataset diretamente da API do Kaggle e salva na camada raw (gera os arquivos test.csv e train.csv dentro de RAW_DIR)

    os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME')
    os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY')

    import kaggle
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(
        KAGGLE_DATASET,
        path=RAW_DIR,
        unzip=True
    )

    print(f"[EXTRACT] Dataset baixado da API do Kaggle e salvo em: {RAW_DIR}")


def transform():
    #Leitura dos arquivos raw

    dfte = pan.read_csv(os.path.join(RAW_DIR, 'test.csv'), sep=',')
    dftr = pan.read_csv(os.path.join(RAW_DIR, 'train.csv'), sep=',')

    dfo = pan.concat([dfte, dftr], ignore_index=True)

    dfo = dfo.rename(columns={
        'Customer_ID': 'Id_do_Cliente',
        'Month': 'Mes',
        'Name': 'Nome',
        'Age': 'Idade',
        'Occupation': 'Profissao',
        'Annual_Income': 'Renda_Anual',
        'Monthly_Inhand_Salary': 'Salario_Liquido_Mensal',
        'Num_Bank_Accounts': 'Numero_de_Contas_Bancarias',
        'Num_Credit_Card': 'Numero_de_Cartoes_de_Credito',
        'Interest_Rate': 'Taxa_de_Juros',
        'Num_of_Loan': 'Numero_de_Emprestimos',
        'Type_of_Loan': 'Tipo_de_Emprestimo',
        'Delay_from_due_date': 'Atraso_na_Data_de_Vencimento',
        'Num_of_Delayed_Payment': 'Numero_de_Pagamentos_Atrasados',
        'Changed_Credit_Limit': 'Quantidade_de_Mudancas_de_Limite',
        'Num_Credit_Inquiries': 'Numero_de_Consultas_de_Credito',
        'Credit_Mix': 'Tipos_de_Creditos',
        'Outstanding_Debt': 'Saldo_Devedor',
        'Credit_Utilization_Ratio': 'Taxa_de_Utilizacao_de_Credito',
        'Credit_History_Age': 'Tempo_de_Uso_de_Credito_em_Meses',
        'Payment_of_Min_Amount': 'Pagamento_do_Valor_Minimo',
        'Total_EMI_per_month': 'Valor_Total_em_Parcelas_Mensais',
        'Amount_invested_monthly': 'Valor_Total_Investido_Mensalmente',
        'Payment_Behaviour': 'Comportamento_de_Pagamento',
        'Monthly_Balance': 'Saldo_Mensal',
        'Credit_Score': 'Pontuacao_de_Credito'
    })

    dfo["Nome"] = dfo["Nome"].str.replace(r'[^a-zA-Z\s\.]', '', regex=True)
    dfo["Nome"] = dfo["Nome"].str.title()

    dfo["SSN"] = dfo["SSN"].str.replace(r'[^0-9]', '', regex=True)

    dfo["Profissao"] = dfo["Profissao"].str.replace(r'[^a-zA-Z\s]', ' ', regex=True)
    dfo["Profissao"] = dfo["Profissao"].str.strip()
    dfo["Profissao"] = dfo["Profissao"].replace(["Valor ausente", "", " "], None)

    dfo["Idade"] = dfo["Idade"].str.replace(r'[^0-9]', '', regex=True)

    dfo["Renda_Anual"] = dfo["Renda_Anual"].str.replace(r'[^0-9\.]', '', regex=True)

    dfo["Numero_de_Emprestimos"] = dfo["Numero_de_Emprestimos"].str.replace(r'[^0-9]', '', regex=True)

    dfo["Numero_de_Pagamentos_Atrasados"] = dfo["Numero_de_Pagamentos_Atrasados"].str.replace(r'[^0-9]', '', regex=True)

    dfo["Quantidade_de_Mudancas_de_Limite"] = dfo["Quantidade_de_Mudancas_de_Limite"].str.replace(r'[^0-9\.]', '', regex=True)

    dfo["Tipos_de_Creditos"] = dfo["Tipos_de_Creditos"].str.replace(r'[^a-zA-Z\s\n]', '', regex=True)
    dfo["Tipos_de_Creditos"] = dfo["Tipos_de_Creditos"].str.strip()

    dfo["Saldo_Devedor"] = dfo["Saldo_Devedor"].str.replace(r'[^0-9\.]', '', regex=True)

    dfo["Valor_Total_Investido_Mensalmente"] = dfo["Valor_Total_Investido_Mensalmente"].str.replace(r'[^0-9\.]', '', regex=True)

    dfo["Comportamento_de_Pagamento"] = dfo["Comportamento_de_Pagamento"].str.replace(r'[^a-zA-Z\,\_]', '', regex=True)

    dfo["Idade"] = pan.to_numeric(dfo["Idade"], errors="coerce")

    dfo["Numero_de_Pagamentos_Atrasados"] = pan.to_numeric(dfo["Numero_de_Pagamentos_Atrasados"], errors="coerce").astype("Float64")

    dfo["Quantidade_de_Mudancas_de_Limite"] = pan.to_numeric(dfo["Quantidade_de_Mudancas_de_Limite"], errors="coerce").round(0).astype("Int64")

    dfo["Numero_de_Consultas_de_Credito"] = pan.to_numeric(dfo["Numero_de_Consultas_de_Credito"], errors="coerce").round(0).astype("Int64")

    dfo["Atraso_na_Data_de_Vencimento"] = pan.to_numeric(dfo["Atraso_na_Data_de_Vencimento"], errors="coerce").round(0).astype("Int64")

    dfo["Saldo_Devedor"] = pan.to_numeric(dfo["Saldo_Devedor"], errors="coerce").round(2)

    dfo["Valor_Total_Investido_Mensalmente"] = pan.to_numeric(dfo["Valor_Total_Investido_Mensalmente"], errors="coerce").round(2)

    dfo["Taxa_de_Utilizacao_de_Credito"] = pan.to_numeric(dfo["Taxa_de_Utilizacao_de_Credito"], errors="coerce").round(2)

    dfo["Numero_de_Emprestimos"] = pan.to_numeric(dfo["Numero_de_Emprestimos"], errors="coerce").fillna(0)

    dfo["Valor_Total_em_Parcelas_Mensais"] = pan.to_numeric(dfo["Valor_Total_em_Parcelas_Mensais"], errors="coerce").round(2)

    dfo["Saldo_Mensal"] = pan.to_numeric(dfo["Saldo_Mensal"], errors="coerce").round(2)

    dfo["Salario_Liquido_Mensal"] = pan.to_numeric(dfo["Salario_Liquido_Mensal"], errors="coerce").round(2)

    dfo["Numero_de_Cartoes_de_Credito"] = pan.to_numeric(dfo["Numero_de_Cartoes_de_Credito"], errors="coerce")

    dfo["Renda_Anual"] = pan.to_numeric(dfo["Renda_Anual"], errors="coerce").round(2)

    anos = dfo["Tempo_de_Uso_de_Credito_em_Meses"].str.extract(r'(\d+)\s+Year').astype(float)

    meses = dfo["Tempo_de_Uso_de_Credito_em_Meses"].str.extract(r'(\d+)\s+Month').astype(float)

    dfo["Tempo_de_Uso_de_Credito_em_Meses"] = anos[0] * 12 + meses[0]

    gbs = ["Nome", "SSN", "Profissao", "Numero_de_Contas_Bancarias", "Taxa_de_Juros", "Numero_de_Emprestimos", "Renda_Anual", "Tipos_de_Creditos", "Pontuacao_de_Credito"]
    for ana in gbs:
        dfo[ana] = dfo.groupby("Id_do_Cliente")[ana].transform(
            lambda x: x.mode()[0] if not x.mode().empty else np.nan
        )

    gbn = ["Salario_Liquido_Mensal", "Numero_de_Pagamentos_Atrasados", "Quantidade_de_Mudancas_de_Limite", "Numero_de_Consultas_de_Credito"]
    for ana in gbn:
        dfo[ana] = dfo.groupby("Id_do_Cliente")[ana].ffill().bfill()

    dfo["Tempo_de_Uso_de_Credito_em_Meses"] = dfo.groupby("Id_do_Cliente")["Tempo_de_Uso_de_Credito_em_Meses"].transform(
        lambda x: x.interpolate(method="linear").ffill().bfill()
    )

    dfo["Tempo_de_Uso_de_Credito_em_Meses"] = dfo["Tempo_de_Uso_de_Credito_em_Meses"].round(0).astype("Int64")

    dfo["Comportamento_de_Pagamento"] = dfo["Comportamento_de_Pagamento"].replace(["Valor ausente", "", " "], None)

    dfo["Comportamento_de_Pagamento"] = dfo.groupby("Id_do_Cliente")["Comportamento_de_Pagamento"].transform(
        lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "Low_spent_Small_value_payments")
    )

    dfo["Valor_Total_Investido_Mensalmente"] = dfo.groupby("Id_do_Cliente")["Valor_Total_Investido_Mensalmente"].transform(
        lambda x: x.fillna(x.median() if not x.dropna().empty else 0)
    )

    dfo["Saldo_Mensal"] = dfo.groupby("Id_do_Cliente")["Saldo_Mensal"].transform(
        lambda x: x.fillna(x.median() if not x.dropna().empty else 0)
    )

    dfo["Tipo_de_Emprestimo"] = np.where(
        dfo["Numero_de_Emprestimos"] == 0, "No Loan", dfo["Tipo_de_Emprestimo"]
    )

    dfo["Atraso_na_Data_de_Vencimento"] = np.where(
        dfo["Atraso_na_Data_de_Vencimento"] < 0, 0, dfo["Atraso_na_Data_de_Vencimento"]
    )

    dfo["Tipos_de_Creditos"] = np.where(
        dfo["Tipos_de_Creditos"] == "", "Not Specific", dfo["Tipos_de_Creditos"]
    )

    tem_emprestimo_mas_esta_vazio = (dfo["Numero_de_Emprestimos"] > 0) & (dfo["Valor_Total_em_Parcelas_Mensais"].isna() | (dfo["Valor_Total_em_Parcelas_Mensais"] == 0))

    dfo.loc[tem_emprestimo_mas_esta_vazio, "Valor_Total_em_Parcelas_Mensais"] = dfo[tem_emprestimo_mas_esta_vazio].groupby("Id_do_Cliente")["Valor_Total_em_Parcelas_Mensais"].transform(
        lambda x: x.fillna(x.median() if not x.dropna().empty else 0)
    )

    dfo.loc[dfo["Numero_de_Emprestimos"] == 0, "Valor_Total_em_Parcelas_Mensais"] = 0

    ordem_meses = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

    dfo["Mes"] = pan.Categorical(dfo["Mes"], categories=ordem_meses, ordered=True)

    dfo = dfo.sort_values(by=["Id_do_Cliente", "Mes"]).reset_index(drop=True)

    dfo.loc[(dfo["Numero_de_Cartoes_de_Credito"] > 10) | (dfo["Numero_de_Cartoes_de_Credito"] < 0), "Numero_de_Cartoes_de_Credito"] = None

    dfo["Numero_de_Cartoes_de_Credito"] = dfo.groupby("Id_do_Cliente")["Numero_de_Cartoes_de_Credito"].transform(
        lambda x: x.fillna(x.median() if not x.dropna().empty else 0)
    )

    dfo["Numero_de_Cartoes_de_Credito"] = dfo["Numero_de_Cartoes_de_Credito"].round(0).astype("Int64")

    dfo.loc[(dfo["Idade"] < 0) | (dfo["Idade"] > 56), "Idade"] = None

    dfo["Idade"] = dfo.groupby("Id_do_Cliente")["Idade"].transform(
        lambda x: x.ffill().bfill()
    )

    dfo["Idade"] = dfo.groupby("Id_do_Cliente")["Idade"].transform(
        lambda x: x.fillna(x.median() if not x.dropna().empty else 0)
    )

    dfo["Idade"] = dfo["Idade"].astype("Int64")

    dfo.loc[(dfo["Numero_de_Pagamentos_Atrasados"] > 28) | (dfo["Numero_de_Pagamentos_Atrasados"] < 0), "Numero_de_Pagamentos_Atrasados"] = None

    dfo["Numero_de_Pagamentos_Atrasados"] = dfo.groupby("Id_do_Cliente")["Numero_de_Pagamentos_Atrasados"].transform(
        lambda x: x.fillna(x.median())
    )

    dfo["Numero_de_Pagamentos_Atrasados"] = dfo["Numero_de_Pagamentos_Atrasados"].fillna(0)

    dfo["Numero_de_Pagamentos_Atrasados"] = dfo["Numero_de_Pagamentos_Atrasados"].round(0).astype("Int64")

    conv_bin = {"No": 0, "Yes": 1, "NM": 2}

    dfo["Pagamento_do_Valor_Minimo"] = dfo["Pagamento_do_Valor_Minimo"].map(conv_bin).astype("Int64")

    # Salva a camada bronze
    os.makedirs(BRONZE_DIR, exist_ok=True)
    bronze_path = os.path.join(BRONZE_DIR, 'silver_dfo.csv')
    dfo.to_csv(bronze_path, index=False)
    print(f"[TRANSFORM] Camada bronze salva em: {bronze_path} ({len(dfo)} linhas)")

    # Separação das tabelas
    df_pessoais = dfo[COLUNAS_PESSOAIS].copy()
    df_pessoais = df_pessoais.drop_duplicates(subset=["Id_do_Cliente"], keep="last")

    df_bancarios = dfo[COLUNAS_BANCARIAS].copy()

    os.makedirs(SILVER_DIR, exist_ok=True)
    pessoais_path = os.path.join(SILVER_DIR, 'dados_pessoais.csv')
    bancarios_path = os.path.join(SILVER_DIR, 'dados_bancarios.csv')

    df_pessoais.to_csv(pessoais_path, index=False)
    df_bancarios.to_csv(bancarios_path, index=False)

    print(f"[TRANSFORM] {len(df_pessoais)} clientes únicos salvos em: {pessoais_path}")
    print(f"[TRANSFORM] {len(df_bancarios)} registros bancários salvos em: {bancarios_path}")


def load():
    #Faz o upload dos arquivos da camada silver (dados_pessoais.csv e dados_bancarios.csv) para o bucket S3 configurado. 

    import boto3

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name='us-east-1'
    )

    pessoais_path = os.path.join(SILVER_DIR, 'dados_pessoais.csv')
    bancarios_path = os.path.join(SILVER_DIR, 'dados_bancarios.csv')

    s3.upload_file(pessoais_path, S3_BUCKET, 'silver/dados_pessoais.csv')
    s3.upload_file(bancarios_path, S3_BUCKET, 'silver/dados_bancarios.csv')

    print(f"[LOAD] Upload concluído para o bucket S3: {S3_BUCKET}")