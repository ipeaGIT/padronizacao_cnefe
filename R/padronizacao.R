padronizar_cnefe <- function() {
  colunas_a_manter <- c(
    "code_address",       # identificador
    "code_state",         # estado
    "code_muni",          # municipio
    "cep",                # cep
    "desc_localidade",    # bairro, povoado, vila, etc
    "nom_tipo_seglogr",   # tipo de logradouro
    "nom_titulo_seglogr", # titulo (e.g. general, papa, santa, etc)
    "nom_seglogr",        # logradouro
    "num_adress",         # numero
    "dsc_modificador",    # modificador do numero
    "cep",                # cep
    "lon",                # longitude
    "lat",                # latituted
    "nv_geo_coord"        # nivel de geocodificacao
  )
}
