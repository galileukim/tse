# set-up ------------------------------------------------------------------
lapply(
  c(
    "tidyverse",
    "R.utils",
    "data.table",
    "stringi",
    "foreach",
    "here"
  ),
  require,
  character.only = T
)

set.seed(1789)

# prep files --------------------------------------------------------------
# conversion ibge-tse
identifiers <- fread(
  "./files/cod_ibge_tse.csv",
  colClasses = c("cod_tse" = "character")
)

# read-in -----------------------------------------------------------------
# donations received
declaracao <- list.files(
  here("data/raw/declaracao/prestacao_contas_final_2016/"),
  pattern = "^receitas_candidatos",
  full.names = T
  ) %>% 
  gunzip(
    remove = F,
    temporary = T,
    overwrite = T
  ) %>% 
  fread(
    integer64 = "character",
    encoding = "Latin-1",
    colClasses = c("Sigla da UE" = "character")
  )

# fix col names
colnames(declaracao) <- colnames(declaracao) %>% 
  stri_trans_general("latin-ascii") %>% 
  str_to_lower %>% 
  str_replace_all(
    "\\s+", "_"
  )

# subset and rename
declaracao <- declaracao %>%
  transmute(
    cod_tse = sigla_da_ue,
    year = 2016,
    mun_name = nome_da_ue,
    candidate_num = numero_candidato,
    candidate_name = nome_candidato,
    position = cargo,
    cpf = cpf_do_candidato,
    receipt_number = numero_recibo_eleitoral,
    donator_cpf = `cpf/cnpj_do_doador`,
    donator_name = nome_do_doador,
    donator_cod_tse = sigla_ue_doador,
    donator_party = numero_partido_doador,
    donator_candidate_number = numero_candidato_doador,
    donator_cnae = cod_setor_economico_do_doador,
    receipt_value = valor_receita,
    receipt_date = data_da_receita,
    receipt_type = tipo_receita,
    donation_source = fonte_recurso,
    donation_type = especie_recurso,
    political_donator_cpf = `cpf/cnpj_do_doador_originario`,
    political_donator_name = nome_do_doador_originario,
    political_donator_type = tipo_doador_originario,
    political_donator_cnae = setor_economico_do_doador_originario
  )

# fix cols
declaracao <- declaracao %>% 
  mutate_if(
    is.character,
    str_to_lower
  ) %>% 
  mutate(
    mun_name = stri_trans_general(mun_name, "latin-ascii")
  )

# join ibge-tse
declaracao <- declaracao %>%
  left_join(
    identifiers,
    by = "cod_tse"
  ) %>% 
  left_join(
    identifiers,
    by = c("donator_cod_tse" = "cod_tse")
  )

# fix col names
declaracao <- declaracao %>%
  rename(
    cod_ibge_6 = cod_ibge_6.x,
    donator_cod_ibge_6 = cod_ibge_6.y
  )

# compress and write out
declaracao %>% 
  fwrite(
    here("data/wrangle/declaracao_receita.csv")
  )

gzip(
  here("data/wrangle/declaracao_receita.csv")
)
