
# parties -----------------------------------------------------------------
coligacao <- fread(
  here('data/wrangle/coalition.csv'),
  na.strings = ""
) 

party <-  list.files(
  here("data/raw/election"),
  pattern = "^party_[local|fed]+",
  full.names = T
) %>%
  map(
    . %>% gunzip(
      remove = F,
      temporary = T,
      overwrite = T
    ) %>%
      fread(
        nThread = parallel::detectCores() - 1,
        encoding = "Latin-1",
        integer64 = "character",
        colClasses = c("CODIGO_MUNICIPIO" = "character")
      )
  ) %>%
  rbindlist

# fix ne and nulo
party[party =="#NE#"|party == "#NULO#"|party == ""] <- NA

# fix tse code
party <- party %>% 
  mutate(
    CODIGO_MUNICIPIO = str_pad(CODIGO_MUNICIPIO, 5, "left", "0")
  )

# wrangle elections
names(party) <- names(party) %>%
  str_to_lower()

# select
# note that nominal refers to votes cast for individual candidates
# legend refers to votes cast for parties. only applicable for legislative elections
party <- party %>%
  transmute(
    election_year = ano_eleicao,
    state = sigla_uf,
    election_type = descricao_eleicao,
    cod_tse = codigo_municipio,
    round = num_turno,
    position_code = codigo_cargo,
    position = descricao_cargo,
    party = sigla_partido,
    mun_name = nome_municipio,
    coalition = str_replace_all(composicao_legenda, "-", "") %>%
      str_replace_all(., "\\d+", "") %>% 
      str_replace(., "^\\/\\s", ""),
    coalition_name = nome_coligacao,
    coalition_type = tipo_legenda,
    vote_legend = qtde_votos_legenda,
    vote_nominal = qtde_votos_nominais
  )

# fix year 2002 vote count (legend instead of nominal)
# clearly a typo, since there cannot be legend votes for president, governor or senator
party <- party %>% 
  mutate(
    vote_nominal = case_when(
      election_year == 2002 & vote_legend > 0 & str_detect(position, "^GOVERNADOR|PRESIDENTE") ~ as.integer(vote_legend),
      T ~ vote_nominal
    ),
    vote_legend = case_when(
      election_year == 2002 & vote_legend > 0 & str_detect(position, "^GOVERNADOR|PRESIDENTE") ~ as.integer(0),
      T ~ vote_legend
    )
  )

# fix col strings
party <- party %>%
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

# filter out "br" states
party <- party %>% 
  filter(
    state != "br"
  )

# fix coalition type and name
party <- party %>% 
  mutate(
    coalition_type = case_when(
      str_detect(coalition_type, "^c") ~ "coligacao",
      str_detect(coalition_type, "^p")~ "partido isolado",
      T ~ coalition_type
    ),
    coalition = case_when(
      coalition_type == "partido isolado" ~ party,
      T ~ coalition
    )
  )

# group vars
group_var <- party %>%
  {names(.)[!str_detect(names(.), "vote")]}

# summarise
party <- party %>%
  group_by_at(
    vars(one_of(group_var))
  ) %>%
  summarise(
    vote_legend = sum(vote_legend),
    vote_nominal = sum(vote_nominal)
  ) %>%
  ungroup()

# fix duplicates
party <- party %>% 
  group_by(
    election_type, 
    election_year, 
    cod_tse, 
    position_code, 
    round, 
    party
  ) %>% 
  mutate(
    n = n()
  ) %>% 
  ungroup()

# extract duplicates
party_dup <- party %>% 
  filter(n > 1)

# fix coalition names
party_dup <- party_dup %>% 
  group_by(
    election_type, 
    election_year,
    state,
    cod_tse, 
    position,
    position_code, 
    round, 
    party,
    mun_name
  ) %>% 
  mutate_at(
    vars(starts_with("coalition")),
    funs(.[!is.na(.)])
  ) %>% 
  group_by(
    coalition,
    coalition_name,
    coalition_type,
    add = T
  ) %>% 
  summarise(
    vote_legend = sum(vote_legend),
    vote_nominal = sum(vote_nominal),
    n = sum(n)
  )

# rebind
party <- party %>% 
  filter(
    n == 1
  ) %>% 
  bind_rows(
    party_dup
  ) %>% 
  select(-n)

# rearrange
party <- party %>% 
  arrange(
    election_year,
    election_type,
    state,
    cod_tse, 
    position,
    position_code, 
    round, 
    party,
    mun_name,
    coalition,
    coalition_name,
    coalition_type
  )

# join ibge-tse
# around 4k obs. mismatched
party <- party %>%
  left_join(
    identifiers,
    by = "cod_tse"
  )

# join with elections, fix coalition
party <- party %>%
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
party <- party %>%
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

# fix coalition and state party code
party <- party %>%
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
      is.na(coalition_type.x) & position_code <= 7 ~ coalition_type,
      is.na(coalition_type.x) & position_code > 7 ~ coalition_type.y,
      TRUE ~ coalition_type.x
    )
  ) %>%
  select(
    -coalition.x,
    -coalition.y,
    -starts_with("coalition_type."),
    -starts_with("coalition_name.")
  )

# note that we don't have coalition information for 13.9% of obs.
party %>% 
  fwrite(
    here("data/wrangle/party.csv")
  )

gzip(
  here("data/wrangle/party.csv"),
  overwrite = T
)
