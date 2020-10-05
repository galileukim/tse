# elections ---------------------------------------------------------------
coligacao <- fread(
  here("data/wrangle/coalition.csv"),
  colClasses = c("cod_tse" = "character")
)

# wrangle and write-out elections
foreach(i = 1:2)%do%{
  # file-names
  election_original <-
    list.files(
      here("data/raw/election"),
      pattern = sprintf("^election_%s", c("fed_state", "local")[i]),
      full.names = T
    ) %>%
    fread(
      nThread = parallel::detectCores() - 1,
      encoding = "Latin-1",
      integer64 = "character",
      colClasses = c("CODIGO_MUNICIPIO" = "character")
    )
  
  # fix ne and nulo
  election_original[election_original == "#NE#"|election_original == "#NULO#"] <- NA
  election_original[election_original == ""] <- NA
  
  # wrangle elections
  names(election_original) <- names(election_original) %>%
    str_to_lower()
  
# aggregate earlier, only removing zone identifier
# group vars
  group_var <- election_original %>%
    {names(.)[!str_detect(names(.), "numero_zona|total_votos")]}
  
  # summarise
  election <- election_original %>%
    group_by_at(
      vars(one_of(group_var))
    ) %>%
    summarise(
      total_votos = sum(total_votos)
    ) %>%
    ungroup()

  # select
  election <- election %>%
    select(
      election_year = ano_eleicao,
      state = sigla_uf,
      election_type = descricao_eleicao,
      cod_tse = codigo_municipio,
      candidate_name = nome_candidato,
      candidate_num = numero_cand,
      candidate_status = desc_sit_candidato,
      round = num_turno,
      position_code = codigo_cargo,
      position = descricao_cargo,
      party = sigla_partido,
      mun_name = nome_municipio,
      candidate_name = nome_candidato,
      coalition = composicao_legenda,
      coalition_name = nome_coligacao,
      outcome = desc_sit_cand_tot,
      vote = total_votos
    )

  # drop 0 votes (only in 2014)
   election <- election %>% 
    filter(
      !(election_year == 2014 & vote == 0)
    )
  # fix col strings
  election <- election %>%
    mutate_if(
      is.character,
      funs(
        stringi::stri_trans_general(
          .,
          "latin-ascii"
        )
      )
    ) %>%
    mutate_if(
      is.character,
      str_to_lower
    )
  
  # fix presidential elections in 2002
  # 2nd round defective and double-counted
  if(i == 1){
    election <- election %>% 
      filter(
        !(position_code == 1 & round == 2 & outcome == "2º turno")
      )
  }
  
  # join ibge-tse
  election <- election %>%
    left_join(
      identifiers,
      by = "cod_tse"
    )
    
  # fix coalition
  election <- election %>%
    left_join(
      coligacao %>%
        filter(
          election_year %in% seq(2000, 2016, 4)
        ),
      by = c(
        "election_year",
        "state",
        "cod_tse",
        "election_type",
        "round",
        "position_code",
        "party"
      )
    )
  
  # state and fed elections
  election <- election %>%
    left_join(
      coligacao %>%
        filter(
          election_year %in% seq(2002, 2014, 4)
        ) %>% 
        select(-cod_tse),
      by = c(
        "election_year",
        "state",
        "election_type",
        "round",
        "position_code",
        "party"
      )
    )
  
  # fix coalition and state election code
  election <- election %>%
    mutate(
      coalition = case_when(
        is.na(coalition.x) & position_code <= 7 ~ coalition,
        is.na(coalition.x) & position_code > 7 ~ coalition.y,
        TRUE ~ coalition.x
      ),
      coalition_name = case_when(
        is.na(coalition_name.x) & position_code <= 7 ~ coalition_name,
        is.na(coalition_name.x) & position_code > 7 ~ coalition_name.y,
        TRUE ~ coalition_name.x
      ),
      coalition_type = case_when(
        is.na(coalition_type.x) ~ coalition_type.y,
        TRUE ~ coalition_type.x
      )
    ) %>%
    select(
      -coalition.x,
      -coalition.y,
      -starts_with("coalition_type."),
      -starts_with("coalition_name.")
    )

  # rbind and write-out
  election %>%
    fwrite(
      sprintf(
        here("data/wrangle/election_%s.csv.gz"), c("fed_state", "local")[i]),
        compress = "gzip"
    )
}