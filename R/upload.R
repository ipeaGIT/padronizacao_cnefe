# arqs <- tar_read(uniao_agregados)
# versao_dados <- tar_read(versao_dados)
upload_arquivos <- function(arqs, versao_dados) {
  # NOTA: NÃO ESTOU CONSEGUINDO CRIAR O RELEASE E FAZER O UPLOAD DEPOIS PELO
  # PIGGYBACK. ELE ATÉ CRIA O RELEASE, MAS NA HORA DE FAZER O UPLOAD NÃO O
  # ENCONTRA. O PROBLEMA NÃO É COM O SYS.SLEEP(), JÁ TESTEI COM VÁRIOS VALORES
  # BEM MAIS ALTOS E CONTINUOU NÃO FUNCIONANDO. O QUE FIZ FOI CRAR O RELEASE
  # COM O CÓDIGO ABAIXO MANUALMENTE E DEPOIS RODAR O RESTO DO CÓDIGO (O QUE
  # ESTÁ FORA DOS COMENTÁRIOS) NO PIPELINE.
  #
  # TL;DR: CRIAR O RELEASE NA MÃO, USANDO O CÓDIGO ABAIXO, E RODAR O TARGETS
  # DEPOIS

  # # tenta criar um release pra fazer upload do cnefe padronizado. se release já
  # # existe a função retorna um warning. no caso, fazemos um "upgrade" de warning
  # # pra erro

  # tryCatch(
  #   piggyback::pb_release_create(
  #     "ipeaGIT/padronizacao_cnefe",
  #     tag = versao_dados,
  #     body = paste0("CNEFE padronizado ", versao_dados)
  #   ),
  #   warning = function(cnd) erro_release_existente(versao_dados)
  # )

  # # o github tem um pequeno lagzinho pra identificar que o release foi criado,
  # # então nós damos um sleep de 2 segundos aqui só pra garantir que a chamada
  # # abaixo da pb_upload() não dá ruim.
  # # issue relacionado: https://github.com/ropensci/piggyback/issues/101
  # Sys.sleep(20)

  purrr::walk(
    arqs,
    function(arq) {
      piggyback::pb_upload(
        arq,
        repo = "ipeaGIT/padronizacao_cnefe",
        tag = versao_dados
      )
    }
  )

  endereco_release <- paste0(
    "https://github.com/ipeaGIT/padronizacao_cnefe/releases/",
    versao_dados
  )

  return(endereco_release)
}

erro_release_existente <- function(versao_dados) {
  cli::cli_abort(
    c(
      "O release {.val {versao_dados}} j\u00e1 existe.",
      "i" = "Por favor, use uma nova tag ou apague o release existente."
    ),
    call = rlang::caller_env(n = 5)
  )
}
