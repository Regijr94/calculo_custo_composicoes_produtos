import polars as pl 

database = pl.read_csv('C:/Users/Reginaldo Junior/Downloads/bweb_1t_CE_091020241636/bweb_1t_CE_091020241636.csv', 
                       separator= ';', 
                       encoding= 'ISO-8859-1', 
                       infer_schema_length=10000)

print(database.columns)
