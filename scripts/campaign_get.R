# get ---------------------------------------------------------------------
lapply(
  c("rcurl", "tidyverse"),
  require,
  character.only = T
)

files_url <- paste0(
  "http://agencia.tse.jus.br/estatistica/sead/odsele/prestacao_contas/prestacao_contas_",
  seq(2002, 2010, 2),
  ".zip"
  ) %>% 
  c(
    paste0(
      "http://agencia.tse.jus.br/estatistica/sead/odsele/prestacao_contas/prestacao_final_",
      seq(2012, 2014, 2),
      ".zip"
    ),
    "http://agencia.tse.jus.br/estatistica/sead/odsele/prestacao_contas/prestacao_contas_final_2016.zip",
    "http://agencia.tse.jus.br/estatistica/sead/odsele/prestacao_contas/prestacao_de_contas_eleitorais_candidatos_2018.zip"
  )

for(i in 1:length(files_url)){
  system(
    paste(
      "cd ~/princeton/R/data/tse/data/raw/campaign/prestacao;",
      "curl -O",
      files_url[i]
    )
  )
}
