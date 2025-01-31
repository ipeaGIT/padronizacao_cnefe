# endereco_cnefe <- tar_read(padronizacao)
# versao_dados <- tar_read(versao_dados)
agregar_cnefe <- function(endereco_cnefe, versao_dados) {
  cnefe <- arrow::open_dataset(endereco_cnefe)
  cnefe <- data.table::setDT(collect(cnefe))

  dir_dados <- file.path(
    Sys.getenv("PUBLIC_DATA_PATH"),
    "CNEFE/cnefe_padrao_geocodebr"
  )

  dir_agreg <- file.path(dir_dados, "2022", versao_dados, "dados_agregados")
  if (!dir.exists(dir_agreg)) dir.create(dir_agreg, recursive = TRUE)

  # temos 12 possíveis casos de agregação. para cada um desses casos,
  # selecionamos as colunas que devem ser usadas e calculamos a média das
  # coordenadas usando essas colunas como grupos

  for (caso in 1:12) {
    cli::cli_progress_step(glue::glue("Agregando caso {caso}"))

    colunas_agregacao <- selecionar_colunas(caso)

    cnefe_agregado <- data.table::copy(cnefe)

    # para os casos de 1 a 4, o número é uma informação relevante. não queremos,
    # portanto, considerar endereços sem número, visto que não é garantido, por
    # exemplo, que dois endereços sem número no mesmo logradouro com mesmo cep e
    # bairro sejam de fato o mesmo endereço. podem ser, por exemplo, dois
    # endereços em extremos opostos da rua, mas igualmente sem número. logo,
    # nesses casos removemos endereços sem número

    if (caso <= 4) cnefe_agregado <- cnefe_agregado[!is.na(numero)]

    # de forma similar, para os casos de 1 a 8 o logradouro é uma informação
    # relevante. o CNEFE usa o nome "SEM DENOMINACAO" para identificar
    # logradouros sem um nome explícito. no entanto, podem existir vários
    # logradouros sem denominação em um mesmo município/bairro, então precisamos
    # removê-los quando o logradouro é uma variável de identificação relevante

    if (caso <= 8) {
      cnefe_agregado <- cnefe_agregado[nome_logradouro != "SEM DENOMINACAO"]
     # cnefe_agregado <- cnefe_agregado[nome_logradouro != "RUA PROJETADA"]
    }

    # agora fazemos a agregação, de fato, usando as colunas selecionadas
    # anteriormente como grupos

    cnefe_agregado <- cnefe_agregado[
      ,
      .(lon = mean(lon), lat = mean(lat)),
      by = colunas_agregacao
    ]

    # adicionamos coluna com o endereço completo, escrito por extenso. essa
    # informação é importante para que os usuários do geocodebr saibam o
    # endereço encontrado a partir do input

    adicionar_coluna_de_endereco(cnefe_agregado, colunas_agregacao)

    data.table::setcolorder(
      cnefe_agregado,
      c(colunas_agregacao, "endereco_completo", "lon", "lat")
    )

    # convertemos de volta para arrow para definirmos o schema e o tipo de cada
    # coluna

    schema_cnefe <- arrow::schema(
      estado = arrow::string(),
      municipio = arrow::string(),
      localidade = arrow::string(),
      cep = arrow::string(),
      numero = arrow::int32(),
      logradouro_sem_numero = arrow::large_utf8(),
      endereco_completo = arrow::large_utf8(),
      lon = arrow::float64(),
      lat = arrow::float64()
    )

    schema_arquivo <- schema_cnefe[
      c(colunas_agregacao, "endereco_completo", "lon", "lat")
    ]

    cnefe_agregado <- arrow::as_arrow_table(
      cnefe_agregado,
      schema = schema_arquivo
    )

    # cada versão agregada é salva com o nome das colunas usadas na agregação.
    # apenas omitimos a coluna "estado", presente em todas as agregações, e
    # substituimos "logradouro_sem_numero" por "logradouro"

    nome_arquivo <- setdiff(colunas_agregacao, "estado")
    nome_arquivo <- sub("logradouro_sem_numero", "logradouro", nome_arquivo)
    nome_arquivo <- paste(nome_arquivo, collapse = "_")
    nome_arquivo <- glue::glue("{nome_arquivo}.parquet")

    endereco_arquivo <- file.path(dir_agreg, nome_arquivo)

    arrow::write_parquet(cnefe_agregado, sink = endereco_arquivo)

    cli::cli_progress_done()
  }

  return(dir_agreg)
}

selecionar_colunas <- function(caso) {
  if (caso == 1) {
    c("estado", "municipio", "logradouro_sem_numero", "numero", "cep", "localidade")
  } else if (caso == 2) {
    c("estado", "municipio", "logradouro_sem_numero", "numero", "cep")
  } else if (caso == 3) {
    c("estado", "municipio", "logradouro_sem_numero", "numero", "localidade")
  } else if (caso == 4) {
    c("estado", "municipio", "logradouro_sem_numero", "numero")
  } else if (caso == 5) {
    c("estado", "municipio", "logradouro_sem_numero", "cep", "localidade")
  } else if (caso == 6) {
    c("estado", "municipio", "logradouro_sem_numero", "cep")
  } else if (caso == 7) {
    c("estado", "municipio", "logradouro_sem_numero", "localidade")
  } else if (caso == 8) {
    c("estado", "municipio", "logradouro_sem_numero")
  } else if (caso == 9) {
    c("estado", "municipio", "cep", "localidade")
  } else if (caso == 10) {
    c("estado", "municipio", "cep")
  } else if (caso == 11) {
    c("estado", "municipio", "localidade")
  } else if (caso == 12) {
    c("estado", "municipio")
  }
}

adicionar_coluna_de_endereco <- function(cnefe_agregado, colunas_agregacao) {
  # padrão de endereço completo:
  #   Av. Venceslau Brás, 72 - Botafogo, Rio de Janeiro - RJ, 22290-140
  #
  # então podemos pensar em 3 "campos":
  # logradouro com número - bairro com muni - estado com cep
  #
  # lembrando também que todos os nossos casos incluem ao menos estado e
  # município

  if (all(c("logradouro_sem_numero", "numero") %in% colunas_agregacao)) {
    cnefe_agregado[, .campo_log := paste0(logradouro_sem_numero, ", ", numero, " - ")]
  } else if ("logradouro_sem_numero" %in% colunas_agregacao) {
    cnefe_agregado[, .campo_log := paste0(logradouro_sem_numero, " - ")]
  } else {
    cnefe_agregado[, .campo_log := ""]
  }

  if ("localidade" %in% colunas_agregacao) {
    cnefe_agregado[, .campo_loc := paste0(localidade, ", ", municipio, " - ")]
  } else {
    cnefe_agregado[, .campo_loc := paste0(municipio, " - ")]
  }

  if ("cep" %in% colunas_agregacao) {
    cnefe_agregado[, .campo_est := paste0(estado, ", ", cep)]
  } else {
    cnefe_agregado[, .campo_est := estado]
  }

  cnefe_agregado[
    ,
    endereco_completo := paste0(.campo_log, .campo_loc, .campo_est)
  ]

  cnefe_agregado[, c(".campo_log", ".campo_loc", ".campo_est") := NULL]

  invisible(cnefe_agregado[])
}
