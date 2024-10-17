import polars as pl
import os
from decimal import Decimal, getcontext

# Configurando a precisão global
getcontext().prec = 8

# Definir os schemas como dicionários para facilitar a leitura e manipulação
schema_custo_unitario = {
    "Produto": pl.Utf8,
    "custo": pl.Float64,
    "Periodo": pl.Datetime,
    "Emp_Cod": pl.Int64
}

schema_composicao = {
    "Produto": pl.Utf8,
    "Composicao": pl.Utf8,
    "Tipo": pl.Utf8,
    "Fator": pl.Float64,
    'FatorComposicao': pl.Float64,
    "QDE": pl.Float64,
    "Periodo": pl.Datetime,
    "Emp_Cod": pl.Int64
}

schema = {
    "Produto": pl.Int32,
    "Emp_Cod": pl.Int32,
    "Periodo": pl.Datetime,
    "custo_prod": pl.Float64
}

# Cache dataframe vazio
df_cache = pl.DataFrame(schema=[(col, dtype()) for col, dtype in schema.items()])

# Ler o arquivo de custo unitário com otimização
custo = pl.read_csv(
    "C:/Users/Reginaldo Junior/Desktop/Custo_HOst/df_custo_msk.csv",
    schema_overrides=schema_custo_unitario
).with_columns(
    pl.col("Periodo").dt.truncate("1d").alias("Periodo")
)

# Ler o arquivo de composição com otimização
composicao = pl.read_csv(
    "C:/Users/Reginaldo Junior/Desktop/Custo_HOst/df_composicao_msk.csv",
    schema_overrides=schema_composicao
)

df = custo\
    .join(composicao,
    left_on  = ["Produto", "Periodo", "Emp_Cod"],
    right_on = ["Produto", "Periodo", "Emp_Cod"], 
    suffix= '_right',
    how="left").select(['Produto', 'Composicao', 'Tipo', 'Periodo', 'Emp_Cod', 'QDE', 'Fator', 'FatorComposicao'])

df = df.join(custo, left_on= ['Composicao', 'Emp_Cod', 'Periodo'],
             right_on= ["Produto",  "Emp_Cod", "Periodo"],  how = 'left' )


# Calcular "custo_final" diretamente após a junção .... funciona para os não TIPO C
df = df.with_columns(
    pl.when(pl.col('Composicao').is_null()).then(pl.col('Produto')).otherwise(pl.col('Composicao')).alias('Composicao'),
    pl.when(pl.col('QDE').is_null())\
        .then(pl.col('custo')).otherwise( (pl.col("QDE") * pl.col("custo")) / pl.col("Fator") )\
            .fill_null(0).alias("custo_final")
).select(['Produto', 'Composicao', 'Tipo', 'Periodo', 'Emp_Cod', 'QDE', 'Fator',  'FatorComposicao', 'custo', 'custo_final' ])


### Ordenar por "Produto" e "Composicao"
df = df.sort(by=["Produto", "Composicao"], descending=[True, False], nulls_last=[True,True])

#
# Contagem de "Tipo == C" após filtro e agrupamento
df_count = (
    df.filter(pl.col("Tipo") == "C")
    .group_by(["Produto", "Periodo", "Emp_Cod"])
    .agg(pl.len().alias("count_tipo_C"))
    .sort(by=["count_tipo_C"], descending=True)
)

## Configuração global para exibir mais linhas
pl.Config.set_tbl_rows(100)
#
## Função para calcular o custo final, otimizada
def calcula_custo_final(df):
    
    return df.group_by(["Produto", "Periodo", "Emp_Cod"]).agg(
        pl.sum("custo_final").alias("custo_prod")
    )
#

df = df.join(
    df_count,
    left_on=["Produto", "Emp_Cod", "Periodo"],
    right_on=["Produto", "Emp_Cod", "Periodo"],
    how="left"
).select([
    pl.col("Produto"),
    pl.col("Composicao"),
    pl.col("Fator"),
    pl.col("FatorComposicao"),
    pl.col("QDE"),
    pl.col("Periodo"),
    pl.col("Tipo"),
    pl.col("Emp_Cod"),
    pl.col("custo"),
    pl.col("custo_final"),
    pl.col("count_tipo_C")
]).with_columns(
    pl.when(pl.col("count_tipo_C").is_null()).then(0).otherwise(pl.col("count_tipo_C")).alias('count_tipo_C')) 

# Ordenar por "count_tipo_C" ascendente e "Composicao" descendente
df = df.sort(
    by=["count_tipo_C", "Composicao"],
    descending=[False, True]
)


produtos2 = df.filter(pl.col("Tipo") == "C")
#
# #Selecionar colunas, eliminar duplicatas e ordenar
lista_c = (
    produtos2
    .select(["Composicao", "Emp_Cod", "Periodo", "count_tipo_C"])  # Selecionar as colunas
    .unique()  # Eliminar duplicatas (equivalente ao distinct)
    .sort(by=["count_tipo_C", "Composicao"], descending=[False, True])  # Ordenar por count_tipo_C ascendente e Composicao descendente
)
#print(lista_c)
### Função corrigida para cálculo do custo secundário
def calculo_custo_secundario(produto, Emp_Cod, periodo, df):
    # Filtra o DataFrame com base nos parâmetros fornecidos
    resultado = df.filter(
        (pl.col("Produto") == produto) &
        (pl.col("Emp_Cod") == Emp_Cod) &
        (pl.col("Periodo") == periodo)
    )

    # Verifica se o DataFrame filtrado está vazio
    if resultado.is_empty():
        # Se estiver vazio, cria uma linha com "custo_prod" igual a 0
        empty_row = (
            pl.DataFrame(
                {
                    "Produto": [produto],
                    "Emp_Cod": [Emp_Cod],
                    "Periodo": [periodo],
                    "custo_prod": [0]
                }
            )
            .with_columns([
                pl.lit(None).alias(col) for col in df.columns if col not in ["Produto", "Emp_Cod", "Periodo", "custo_final"]
            ])
        )
        return empty_row

    # Caso contrário, agrupa e soma os valores de "custo_final"
    custo_final = resultado.group_by(
        ["Produto", "Emp_Cod", "Periodo"]
    ).agg(
        pl.sum("custo_final").fill_null(0).alias("custo_prod")
    )

    return custo_final

# Função corrigida para captura de FatorComposicao
def captura_FatorComposicao(produto, Emp_Cod, periodo, df):
    # Certifique-se de que o FatorComposicao tenha uma chave de junção válida
    return (
        df.filter(
            (pl.col("Composicao") == produto) &
            (pl.col("Emp_Cod") == Emp_Cod) &
            (pl.col("Periodo") == periodo)
        )
        .select(pl.col("Fator"))
        .limit(1)
    )
    
    
def captura_qde(produto, Emp_Cod, periodo, df):
    # Certifique-se de que o FatorComposicao tenha uma chave de junção válida
    return (
        df.filter(
            (pl.col("Composicao") == produto) &
            (pl.col("Emp_Cod") == Emp_Cod) &
            (pl.col("Periodo") == periodo)
        )
        .select(pl.col("QDE"))
        .limit(1)
    )    

# Função corrigida para anti-join e atualização
def atualiza_df(df_antigo, df_novo):
    
    # Renomear colunas para evitar colisão de nomes
    df_novo = df_novo.rename({
        "Produto": "Produto_novo",
        "Emp_Cod": "Emp_Cod_novo",
        "Periodo": "Periodo_novo"
    })

    # Realizando a junção anti
    df_new = df_antigo.join(
        df_novo,
        left_on=["Composicao", "Emp_Cod", "Periodo"],
        right_on=["Produto_novo", "Emp_Cod_novo", "Periodo_novo"],
        how="anti"
    ).select(
        "Produto", "Composicao", "Emp_Cod", "Periodo", "Tipo", "QDE", "Fator", "FatorComposicao", "custo", "custo_final"
    )

    #  pega intersecção
    df_joined = df_antigo.join(
        df_novo,
        left_on=["Composicao", "Emp_Cod", "Periodo"],
        right_on=["Produto_novo", "Emp_Cod_novo", "Periodo_novo"],
        how="inner"
    ).with_columns(
        pl.when(pl.col("Tipo") == "C").then(  pl.col("custo_prod") ).otherwise(pl.col("custo_final")).alias("custo_final")
    ).select(
        "Produto", "Composicao", "Emp_Cod", "Periodo", "Tipo", "QDE", "Fator", "FatorComposicao", "custo", "custo_final"
    )

    # Combinando os DataFrames
    df_result = df_new.vstack(df_joined)

    return df_result

# Itera sobre as linhas de `lista_c`
for row in lista_c.iter_rows():
    
    Composicao = row[0]  # Composicao
    empcod   = row[1]    # Emp_Cod
    periodo  = row[2]   # Periodo
   
    try:
        # Filtra a composição do Composicao tipo C
        df_filtro = calculo_custo_secundario(Composicao, empcod, periodo, df)
   
       
        # Verifica se há resultados
        n = df_filtro.height  # Use .height para contar o número de linhas
        
        if n > 0:
            # Captura o FatorComposicao
            FatorComposicao = captura_FatorComposicao(Composicao, empcod, periodo, df)
            
            # Realiza a junção com o FatorComposicao e calcula o custo_prod
            
            df_custo = df_filtro.join(FatorComposicao, how="cross")
            
            
            
            df_custo= df_custo.with_columns(
                ( ( pl.col("custo_prod")  )  ).alias("custo_prod")
            )
                        
            # Atualiza o DataFrame original
            df = atualiza_df(df, df_custo)
            
                                    
                 
    except Exception as e:
        # Captura e ignora erros, como no código original
        continue

df = df.with_columns(
    (pl.col('custo_final').sum().over('Produto') / pl.col('Fator')).alias('total')
)

#print(df.filter(pl.col('Produto')=='857'))
## Caminho do arquivo de saída
file_path = "C:/Users/Reginaldo Junior/Desktop/Custo_HOst/custo_misaki_1024.csv"
#
if os.path.exists(file_path):
    os.remove(file_path)

df.write_csv(file_path, separator= "|")
