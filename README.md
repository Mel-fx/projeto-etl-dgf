# projeto-etl-dgf
# Documentação Técnica — ETL Automatizado

Projeto Final da Trilha de Engenharia de Dados — Bootcamp [RE]Start
Empresa fictícia: Data Girls Finance

## 1. Extração de Dados

A extração foi via **API oficial do Kaggle** (biblioteca `kaggle` fornecido).

**Implementação:** a função `extract()` autentica na API usando credenciais fornecidas por
variáveis de ambiente (`KAGGLE_USERNAME`, `KAGGLE_KEY`) e baixa o dataset
`parisrohan/credit-score-classification`, salvando os arquivos `test.csv` e `train.csv` na
camada `raw/` do projeto.

**Justificativa da escolha:** essa abordagem evita expor ou versionar arquivos de dados
potencialmente grandes no GitHub, e torna o pipeline reprodutível por qualquer pessoa que
tenha suas próprias credenciais configuradas, sem depender de um download manual prévio.

## 2. Transformação de Dados

A transformação foi feita inteiramente em **Python (pandas e numpy)**, dentro da função
`transform()`. As etapas aplicadas, na ordem em que ocorrem no código:

1. **Unificação:** os arquivos `test.csv` e `train.csv` são concatenados em um único
   DataFrame (`dfo`), eles representam o mesmo conjunto de clientes dividido em duas
   partes (train: janeiro à agosto e test: setembro à dezembro) pelo autor original do dataset.

2. **Renomeação de colunas:** todas as colunas originais (em inglês) foram traduzidas para
   nomes em português, padronizando a nomenclatura usada no restante do pipeline (ex:
   `Customer_ID` → `Id_do_Cliente`, `Annual_Income` → `Renda_Anual`).

3. **Limpeza de campos textuais:** colunas como `Nome`, `SSN`, `Profissao`,
   `Tipos_de_Creditos` e `Comportamento_de_Pagamento` continham caracteres inválidos
   (símbolos, dígitos misturados com texto), portanto, foram aplicadas expressões regulares para remover esses caracteres e
   padronizar capitalização (ex: `Nome` convertido para *title case*). Esses símbolos ou caracteres que não eram para estar naquela coluna, podem ser devido a algum erro de digitação ou erro na importação.

4. **Conversão de tipos:** colunas numéricas que vieram como texto (ex: `Idade`,
   `Renda_Anual`, `Saldo_Devedor`) foram limpas de caracteres não numéricos e convertidas
   com `pd.to_numeric(errors="coerce")`, transformando valores inválidos em nulos em vez de
   quebrar a execução.

5. **Conversão de tempo de histórico de crédito:** o campo original vinha em formato de
   texto ("X Years Y Months"). Foi extraído o número de anos e meses via regex e convertido
   para um valor único em meses (`Tempo_de_Uso_de_Credito_em_Meses`). Esse caminho foi escolhido pensando na "facilidade" do dashboard quando vinculado, conseguir ler corretamente e dar uma visualização melhor quando aplicados os filtros.

6. **Tratamento de outliers:** após analisar as colunas, foram identificados outliers e transformados em nulos valores fora de faixas
   plausíveis, como idade negativa ou acima de 56 anos, número de cartões de crédito fora
   da faixa 0–10, e número de pagamentos atrasados fora da faixa 0–28. Esses valores foram
   posteriormente reimputados (ver item 7). 

7. **Preenchimento dos valores ausentes:** a estratégia de preenchimento foi tomada conforme a
   natureza do campo:
   - **Campos estáveis por cliente** (nome, SSN, profissão, tipo de crédito, número de
     contas bancárias, pontuação de crédito, etc.): preenchidos pela **moda** do próprio
     cliente (`groupby("Id_do_Cliente")`), já que esses valores não deveriam variar mês a
     mês e divergências indicam erro de digitação em algumas linhas.
   - **Campos financeiros variáveis** (salário líquido mensal, número de pagamentos
     atrasados, consultas de crédito): imputados por *forward fill*/*backward fill* dentro
     do histórico do próprio cliente. Assim, preenchendo de forma coerente ao histórico do cliente.
   - **Tempo de uso de crédito:** preenchido por interpolação linear dentro do histórico do
     cliente, respeitando o histórico já informado do cliente
   - **Saldo mensal e valor investido mensalmente:** preenchidos pela mediana do cliente,
     evitando distorção por outliers.
   - **Campos Yes or No:** Transformado em binário, para melhor leitura do software.

8. **Regras de negócio específicas:** clientes sem nenhum empréstimo (`Numero_de_Emprestimos
   == 0`) tiveram o campo `Tipo_de_Emprestimo` preenchido como "No Loan" e o valor de
   parcelas mensais zerado, evitando inconsistência entre campos relacionados.

9. **Ordenação:** os registros foram ordenados por `Id_do_Cliente` e `Mes` (respeitando a
   ordem cronológica dos meses, não a ordem alfabética), preservando a coerência temporal
   do histórico de cada cliente.

10. **Separação por domínio:** ao final da limpeza, o DataFrame único foi dividido em duas
    tabelas:
    - `dados_pessoais`: campos cadastrais estáveis (`Id_do_Cliente`, `Nome`, `Idade`, `SSN`,
      `Profissao`), com `drop_duplicates(subset=["Id_do_Cliente"], keep="last")` aplicado
      para reduzir a granularidade mensal a uma linha única por cliente.
    - `dados_bancarios`: todos os campos financeiros/comportamentais, mantidos na
      granularidade mensal original, já que representam uma série temporal por cliente.

**Justificativa da separação:** Pensando na segurança dos dados (LGPD), caso consultem um mês em específico, somente aparecerá o id do cliente junto com as colunas de dados bancários, e não suas informações pessoais. Além disso, foi separado por domínios, devido a diferença de dados (sendo os dados pessoais não variáveis por mês, sendo constantes, e os dados bancários serem variáveis).

## 3. Armazenamento dos Dados em Nuvem

Optou-se pela **AWS S3** como plataforma de armazenamento em nuvem, conforme recomendado.

**Implementação:** a função `load()` utiliza a biblioteca `boto3` para autenticar via
Access Key/Secret Key (fornecidas por variáveis de ambiente) e enviar os dois arquivos da
camada silver (`dados_pessoais.csv` e `dados_bancarios.csv`) para um bucket na região
`sa-east-1` (São Paulo), organizados sob o prefixo `silver/`.

**Estrutura de pastas do projeto:** os dados são organizados localmente em três
subdiretórios (`data/raw/`, `data/bronze/`, `data/silver/`), refletindo as camadas do
pipeline e facilitando auditoria de cada etapa de processamento.

<img width="1722" height="472" alt="{DB5C6B50-D04F-45E5-8987-6E66CDBFBAF6}" src="https://github.com/user-attachments/assets/857ec987-dddb-4f77-abeb-1d132ab3ed17" />

## 4. Automação do Pipeline

O pipeline foi orquestrado com **Apache Airflow**, executado em ambiente **Docker**.

**Estrutura da DAG:** a DAG `etl_credit_score` contém três tasks sequenciais, mapeadas
diretamente às três funções descritas acima:

```
extract_data >> transform_data >> load_to_s3
```

Cada task é um `PythonOperator` que chama uma das funções (`extract`, `transform`, `load`)
definidas em um módulo separado (`etl_functions.py`), mantendo a separação entre lógica de
negócio (o que fazer) e orquestração (quando/como executar).

**Agendamento:** configurado com `schedule_interval='@daily'`, simulando atualizações
periódicas dos dados.

**Ambiente de execução:** foi utilizado um `Dockerfile` customizado, construindo uma
imagem baseada em `apache/airflow:2.9.2` com as dependências do projeto (`pandas`, `boto3`,
`kaggle`) já instaladas de forma permanente, em vez de reinstalá-las a cada inicialização
do container (abordagem inicialmente testada via `_PIP_ADDITIONAL_REQUIREMENTS`, mas
descartada por instabilidade, conforme recomendação da própria documentação do Airflow).

**Logging:** o próprio Airflow registra logs de cada task automaticamente, incluindo
prints informativos adicionados em cada função (`[EXTRACT]`, `[TRANSFORM]`, `[LOAD]`) com
contagem de linhas processadas em cada etapa.

<img width="1857" height="617" alt="{8889D2A4-7F96-4E5E-BBCA-8484AEC1917D}" src="https://github.com/user-attachments/assets/58da9861-3095-4cb6-8ea3-974286fdef5e" />

## 5. Dashboard Power BI (Bônus)

Como entrega opcional, foi desenvolvido um dashboard conectado diretamente aos arquivos CSV
tratados da camada silver.

**Modelagem:** foi criado um relacionamento entre as tabelas `dados_pessoais` e
`dados_bancarios` pela chave `Id_do_Cliente`, com cardinalidade um-para-muitos (1 cliente
para N registros mensais), refletindo a mesma lógica de granularidade do pipeline.

**Visualizações construídas:**
1. **Total de clientes cadastrados** — cartão com contagem distinta de `Id_do_Cliente`
2. **Distribuição dos clientes por classificação de Score de Crédito** — gráfico de pizza
   por `Pontuacao_de_Credito`
3. **Distribuição da renda anual por categoria de score** — gráfico de barras com a
   **média** (não soma) de `Renda_Anual` por `Pontuacao_de_Credito`
   
Além de outras que foram adicionadas para complementar o dashboard (como demonstrado nas imagens a seguir)

**Observação técnica relevante:** a métrica de renda inicialmente foi construída com
agregação de **soma**, o que gerou valores incoerentes (na casa de trilhões). A causa foi
identificada: como `Renda_Anual` está na tabela de granularidade mensal, o mesmo valor de
renda de um cliente se repete em todas as suas linhas mensais; somar essas linhas
multiplicava o valor real pelo número de meses de histórico do cliente. A correção foi
trocar a agregação para **média**, que não é afetada pela repetição do valor idêntico.

<img width="1485" height="809" alt="{69249586-41A7-4B39-834F-E4E80DEADE11}" src="https://github.com/user-attachments/assets/cea0a157-43f2-40dc-adde-239337793012" />

<img width="1485" height="811" alt="{66CE7B20-AD89-442D-A44C-D37323305111}" src="https://github.com/user-attachments/assets/7c262873-fec8-4dd5-b732-13f027c375b3" />

## Perguntas Norteadoras de Negócio

**1. Como garantir que os dados cadastrais e financeiros dos clientes estejam sempre
atualizados e prontos para utilização pelas equipes de negócio?**

A DAG é agendada para execução diária, reprocessando os dados desde a extração via API do
Kaggle até a carga no S3 a cada execução. As equipes de Analytics e Crédito sempre acessam
a versão mais recente processada no bucket, sem depender de atualização manual.

**2. Quais validações de qualidade dos dados devem ser realizadas antes que as informações
sejam disponibilizadas para análises e modelos de score de crédito?**

Foram aplicadas: remoção de caracteres inválidos em campos textuais via regex; conversão e
validação de tipos numéricos com tratamento de erro (`errors='coerce'`); identificação e
nulificação de outliers em campos como idade e número de cartões; e imputação de valores
ausentes usando estatísticas calculadas por cliente (moda para campos estáveis,
mediana/interpolação para campos financeiros variáveis), preservando a consistência dentro
do histórico individual de cada cliente.

**3. Como estruturar um pipeline que permita atualizações periódicas dos dados sem
duplicar registros e preservando sua consistência?**

A tabela de dados pessoais aplica `drop_duplicates` por `Id_do_Cliente` a cada execução,
garantindo uma única linha por cliente independentemente de quantas vezes o pipeline seja
executado. A separação por domínio também evita redundância de dados estáveis sendo
repetidos a cada linha mensal da tabela financeira.

**4. Como organizar e armazenar os dados para facilitar consultas analíticas e alimentar
dashboards ou modelos preditivos de classificação de crédito?**

Os dados foram organizados em camadas (raw, bronze, silver) e separados por domínio
(pessoais e bancários), armazenados em CSV no S3 sob prefixos organizados. Essa separação
facilita joins pontuais por `Id_do_Cliente` quando necessário, como feito na modelagem do
dashboard Power BI, sem forçar ferramentas de análise a lidar com uma tabela única e
redundante.

## Tecnologias Utilizadas

- **Python** (pandas, numpy) — extração e transformação dos dados
- **Kaggle API** — extração automatizada do dataset
- **Apache Airflow** — orquestração e automação do pipeline
- **Docker / Docker Compose** — containerização do ambiente de execução
- **AWS S3** (boto3) — armazenamento em nuvem dos dados tratados
- **Power BI** — visualização e dashboard analítico (bônus)

## Autoria

Projeto desenvolvido por Melina Aguiar como entrega final do Bootcamp [RE]Start — Trilha de
Engenharia de Dados, mentoria Data Girls.
