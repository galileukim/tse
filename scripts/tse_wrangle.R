source(here::here("scripts/tse_setup.R"))
source(here("scripts/modules/coalition_wrangle.R"))
source(here("scripts/modules/party_wrangle.R"))
source(here("scripts/modules/election_wrangle.R"))

# candidates --------------------------------------------------------------
coligacao <- fread(
  here("data/wrangle/coalition.csv"),
  colClasses = c("cod_tse" = "character", "election_year" = "character"),
  na.strings = ""
)

# read-in
candidate <-  list.files(
  here("data/raw/election"),
  pattern = "^candidate_fed|^candidate_local",
  full.names = T
) %>%
  map(
    . %>% 
      gunzip(
        remove = F,
        temporary = T,
        overwrite = T
      ) %>%
      fread(
        nThread = parallel::detectCores() - 1,
        encoding = "Latin-1",
        integer64 = "character",
        colClasses = c("SIGLA_UE" = "character", "ANO_ELEICAO" = "character")
      )
  ) %>% 
  rbindlist(fill = T)

# fix ne and nulo
candidate[candidate == "#NE#"|candidate == "#NULO#"|candidate == ""] <- NA

# select
candidate <- candidate %>%
  transmute(
    cod_tse = as.character(SIGLA_UE),
    election_year = ANO_ELEICAO,
    mun_name = DESCRICAO_UE,
    state = SIGLA_UF,
    election_type = DESCRICAO_ELEICAO,
    round = NUM_TURNO,
    tse_name = DESCRICAO_UE,
    position_code = CODIGO_CARGO,
    position = DESCRICAO_CARGO,
    candidate_name = NOME_CANDIDATO,
    candidate_num = NUMERO_CANDIDATO,
    candidate_status = DES_SITUACAO_CANDIDATURA,
    cpf_candidate = CPF_CANDIDATO,
    party = SIGLA_PARTIDO,
    coalition = COMPOSICAO_LEGENDA,
    coalition_name = NOME_COLIGACAO,
    occupation_code = CODIGO_OCUPACAO,
    occupation = DESCRICAO_OCUPACAO,
    birthyear = str_sub(
      DATA_NASCIMENTO,
      -2, -1
    ) %>%
      paste0(19, .),
    elec_title = NUM_TITULO_ELEITORAL_CANDIDATO,
    age = IDADE_DATA_ELEICAO,
    gender = CODIGO_SEXO,
    edu = COD_GRAU_INSTRUCAO,
    edu_desc = DESCRICAO_GRAU_INSTRUCAO,
    mun_birth_code = CODIGO_MUNICIPIO_NASCIMENTO,
    mun_birth_name = NOME_MUNICIPIO_NASCIMENTO,
    campaign = DESPESA_MAX_CAMPANHA,
    outcome = DESC_SIT_TOT_TURNO,
    email = EMAIL_CANDIDATO,
    race = DESCRICAO_COR_RACA,
    civil_status = DESCRICAO_ESTADO_CIVIL
  ) %>%
  left_join(
    identifiers,
    by = "cod_tse"
  )

# fix year 2014: defective column entries
candidate_2014 <- candidate %>% 
  filter(
   election_year == 2014
  ) %>% 
  rename(
    race = civil_status,
    civil_status = edu_desc,
    edu_desc = race
  )

candidate <- candidate %>% 
  filter(
    election_year != 2014
  ) %>% 
  bind_rows(
    candidate_2014
  ) %>% 
  arrange(
    cod_ibge_6,
    election_year,
    position_code
  )

# fix characters
candidate <- candidate %>%
  mutate_if(
    is.character,
    funs(
      stri_trans_general(
        ., "latin-ascii"
      )
    )
  ) %>%
  mutate_if(
    is.character,
    str_to_lower
  )

# fix coalition
candidate <- candidate %>%
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

# state and fed candidates
candidate <- candidate %>%
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

# fix coalition and state candidate code
candidate <- candidate %>%
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

# fix position labels
candidate <- candidate %>% 
  mutate(
    position = case_when(
      position_code == 9 ~ "1º suplente senador",
      position_code == 10 ~ "2º suplente senador",
      TRUE ~ position
    )
  )

# remove duplicate candidates
# 2001 obs.
candidate <- candidate %>%
  distinct(
    cod_tse,
    candidate_name,
    candidate_num,
    election_type,
    election_year,
    round,
    position_code,
    .keep_all = T
  )

# create reelection indicators
# fix defective outcome entries
candidate <- candidate %>% 
  mutate(
    election_year_lag = as.integer(election_year) - 4,
    outcome = case_when(
      str_detect(candidate_status, "^deferido") & is.na(outcome) ~ NA_character_,
      candidate_status == "indeferido" & is.na(outcome) ~ "indeferido",
      !str_detect(candidate_status, "^deferido|^indeferido") & is.na(outcome) ~ "renuncia/falecimento/cassacao antes da eleicao",
      T ~ outcome
    ),
    elected = if_else(
      str_detect(outcome, "^eleito|media"),
      1, 0
    )
  )

# create times run for/ in office
candidate <- candidate %>% 
  group_by(
    cpf_candidate,
    elec_title,
    candidate_name
  ) %>%
  arrange(
    election_year,
    round
  ) %>% 
  mutate(
    times_run = cumsum(round == 1),
    times_office = cumsum(elected)
  ) %>%
  ungroup()

# create lagged table
# only unique entries on join
# remove supplementary elections
supplementary <- candidate %>% 
  count(election_type) %>% 
  filter(
    n < 1000
  ) %>% 
  pull(
    election_type
  )

candidate_join <- candidate %>% 
  filter(
    !(election_type %in% supplementary),
    !str_detect(outcome, "turno$")
  ) %>% 
  distinct(
    election_year,
    cpf_candidate,
    candidate_name,
    elec_title,
    birthyear,
    cod_tse,
    position,
    .keep_all = T
  )

# create incumbency and reelection status
candidate <- sqldf::sqldf(
  '
  SELECT t1.*, t2.elected AS incumbent, t2.outcome as outcome_previous, t2.position as position_previous
  FROM candidate AS t1 LEFT JOIN candidate_join AS t2 
  ON t1.election_year_lag = t2.election_year AND
  t1.cpf_candidate = t2.cpf_candidate AND
  t1.candidate_name = t2.candidate_name AND
  t1.elec_title = t2.elec_title AND
  t1.birthyear = t2.birthyear AND
  t1.cod_tse = t2.cod_tse AND
  t1.position = t2.position
  '
) %>% 
  mutate(
    local = if_else(
      as.integer(election_year) %% 4 == 0,
      1, 0
    )
  ) %>% 
  group_by(local) %>% 
  mutate(
    incumbent = case_when(
      incumbent == 1 ~ as.integer(1),
      incumbent == 0 | is.na(incumbent) & election_year > min(election_year) ~ as.integer(0),
      incumbent == 0 & election_year == min(election_year) ~ NA_integer_
    )
  ) %>% 
  ungroup() %>%
  select(
    -local,
    -election_year_lag
  )

# reorder
candidate <- candidate %>%
  select(
    order(colnames(.))
  ) %>%
  select(
    cpf_candidate,
    cod_ibge_6,
    election_year,
    everything()
  )

# write-out
candidate %>%
  fwrite(
    here("data/wrangle/candidate.csv")
  )

# compress
gzip(
  here("data/wrangle/candidate.csv"),
  overwrite = T
)

# clean-up
rm(list = ls())
gc()

# join electoral data - candidate -----------------------------------------
# note that we filter election_year >= 2000
election <- list.files(
  here("data/wrangle"),
  "^election_[fed|local].*gz$",
  full.names = T
) %>%
  map(
    . %>%
      gunzip(
        remove = F,
        temporary = T,
        overwrite = T
      ) %>%
      fread(
        nThread = parallel::detectCores() - 1,
        encoding = "UTF-8",
        colClasses = c("cod_tse" = "character"),
        select = c(
          "election_year",
          "cod_tse",
          "party",
          "cod_ibge_6",
          "mun_name",
          "state",
          "election_type",
          "round",
          "position",
          "position_code",
          "candidate_num",
          "candidate_name",
          "candidate_status",
          "coalition",
          "coalition_name",
          "coalition_type",
          "outcome",
          "vote"
        )
      )
  ) %>%
  rbindlist %>% 
  filter(election_year >= 2000)

# candidate
candidate <- list.files(
  here("data/wrangle"),
  "^candidate.csv",
  full.names = T
) %>%
  gunzip(
    temporary = T,
    overwrite = T,
    remove = F
  ) %>%
  fread(
    nThread = parallel::detectCores() - 1,
    encoding = "UTF-8",
    colClasses = c("cod_tse" = "character"),
  ) %>%
  select(
    -tse_name,
    -cod_ibge_6,
    -outcome,
    -elected,
    -starts_with("coalition"),
    -position,
    -party,
    -mun_name,
    -candidate_status
  ) %>% 
  filter(election_year >= 2000)

# remove foreign voting and transit vote
election <- election %>%
  filter(
    cod_tse != "zz",
    state != "zz",
    state != "vt"
  )

# join candidates to election: different levels
election <- election %>% # local
  filter(
    (election_year %% 4) == 0
  ) %>%
  left_join(
    candidate %>%
      select(
        -state
      ),
    by = c(
      "election_year",
      "cod_tse",
      "election_type",
      "round",
      "position_code",
      "candidate_num",
      "candidate_name"
    )
  ) %>% # federal and state
  bind_rows(
    election %>%
      filter(
        (election_year %% 4) == 2,
        position_code != 1
      ) %>%
      left_join(
        candidate %>%
          select(
            -cod_tse
          ),
        by = c(
          "election_year",
          "state",
          "election_type",
          "round",
          "position_code",
          "candidate_num",
          "candidate_name"
        )
      )
  ) %>% # president
  bind_rows(
    election %>%
      filter(
        (election_year %% 4) == 2,
        position_code == 1
      ) %>%
      left_join(
        candidate %>%
          select(
            -cod_tse,
            -state
          ),
        by = c(
          "election_year",
          "election_type",
          "position_code",
          "candidate_num",
          "candidate_name",
          "round"
        )
      )
  )

# fix defective duplicates
# all in the year 2000
# lose 1016 obs.
election <- election %>% 
  distinct(
    cpf_candidate,
    elec_title, 
    birthyear,
    candidate_name, 
    round, 
    election_type,
    cod_tse, 
    position,
    .keep_all = T
  )

# create reelection indicators
# note that there are minor mismatch between candidate-reported and electoral outcomes
# 37285 obs. or 0.29%
# also 3266 obs. are missing outcome in candidate data from mismatch
# additional missing data from candidate dataset is 6894 obs.
# decision: use outcomes from electoral dataset
election <- election %>% 
  mutate(
    elected = if_else(
      str_detect(outcome, "^eleito|media"),
      1, 0
    ),
    reelected = if_else(
      incumbent == 1 & elected == 1,
      1, 0
    )
  )

# rearrange
election <- election %>%
  select(
    order(colnames(.))
  ) %>%
  select(
    cpf_candidate,
    cod_ibge_6,
    election_year,
    everything()
  ) %>%
  arrange(
    election_year,
    cod_ibge_6,
    position_code
  )

# write-out
election %>%
  fwrite(
    here("data/wrangle/election.csv")
  )

# compress
gzip(
  here("data/wrangle/election.csv"),
  overwrite = T
)

# only elected
election %>%
  filter(
    str_detect(
      outcome,
      "^eleito|media"
    )
  ) %>%
  select(
    -outcome
  ) %>%
  fwrite(
    here("data/wrangle/elected.csv")
  )

gzip(
  here("data/wrangle/elected.csv"),
  overwrite = T
)

# clean up
rm(list = ls())
gc()

# chamber -----------------------------------------------------------------
# elections
election <- list.files(
  here("data/wrangle"),
  pattern = "^election.csv",
  full.names = T
) %>%
  fread(
    nThread = parallel::detectCores() - 1,
    select = c(
      "cpf_candidate",
      "elec_title",
      "candidate_name",
      "cod_ibge_6",
      "cod_tse",
      "round",
      "mun_name",
      "election_year",
      "birthyear",
      "candidate_status",
      "party",
      "campaign",
      "edu",
      "gender",
      "edu_desc",
      "coalition",
      "coalition_name",
      "position",
      "position_code",
      "position_previous",
      "occupation",
      "election_type",
      "outcome",
      "outcome_previous",
      "incumbent",
      "elected",
      "reelected",
      "ideology",
      "vote"
    )
  ) %>%
  filter(
    election_year %in% seq(2000, 2016, 4)
  )

# retain municipalities with only one type of election
# rule out problem of supplementary elections
election <- election %>%
  group_by(
    election_year,
    cod_ibge_6
  ) %>%
  mutate(
    n = n_distinct(election_type)
  ) %>%
  ungroup() %>% 
  filter(n == 1) %>%
  select(-n)

# extract municipalities with no second round
election <- election %>%
  group_by(
    cod_ibge_6,
    election_year
  ) %>%
  mutate(
    round_two = !any(round == 2)
  ) %>%
  ungroup() %>% 
  filter(
    round_two
  ) %>% 
  select(
    -starts_with("round")
  ) %>%
  ungroup

# eliminate defective entries with no elected officials
election <- election %>%
  group_by(
    cod_tse,
    election_type,
    position
  ) %>%
  filter(
    any(str_detect(outcome, "^eleito|media"))
  ) %>%
  ungroup()

# add municipal votes for mayor election
election <- election %>% 
  group_by(
    cod_ibge_6,
    election_year
  ) %>% 
  mutate(
    total_vote_mayor = sum(vote[position == "prefeito"], na.rm = T),
    total_vote_legislative = sum(vote[position == "vereador"], na.rm = T)
  ) %>% 
  ungroup()

# victory margin
election <- election %>%
  group_by(
    election_year,
    cod_ibge_6
  ) %>%
  arrange(
    desc(vote),
    desc(position_code),
    .by_group = T
  ) %>%
  mutate(
    vote_margin =
      (vote[row_number() == 1] - vote[row_number() == 2])/
      vote[row_number() == 2]
  ) %>%
  arrange(
    election_year,
    cod_ibge_6,
    position_code
  ) %>%
  ungroup()

# retain elected officials
election <- election %>%
  filter(
    elected == 1
  )

# coalition size and age
election <- election %>%
  mutate(
    coalition_size = 1 + str_count(coalition, "/"),
    age = election_year - as.integer(birthyear)
  )

# mayor coalition
# fix sao miguel do tapuio
# strangely there are some municipalities without mayor: 8 in total
# prob. because I eliminated supplementary election wins
election <- election %>%
  filter(
    cod_ibge_6 != 221040
  ) %>%
  group_by(
    election_year,
    cod_ibge_6
  ) %>%
  mutate(
    mayor = sum(position == "prefeito", na.rm = T)
  ) %>%
  filter(
    mayor == 1
  ) %>%
  mutate(
    mayor_party = party[position == "prefeito"],
    mayor_coalition = coalition[position == "prefeito"]
  ) %>%
  select(-mayor) %>%
  ungroup()

# mayor party vars
election <- election %>%
  group_by(
    election_year,
    cod_ibge_6
  ) %>%
  mutate(
    mayor_party_share = sum(
      party == mayor_party & position == "vereador"
    )/sum(position == "vereador"),
    mayor_party_margin = mayor_party_share - 0.5,
    mayor_party_majority = if_else(
      mayor_party_share > 0.5, 1, 0
    ),
    mayor_party_edu = mean(
      edu[position == "vereador" & party == mayor_party],
      na.rm = T
    ),
    mayor_party_campaign = mean(
      campaign[position == "vereador" & party == mayor_party],
      na.rm = T
    ),
    mayor_party_age = mean(
      age[position == "vereador" & party == mayor_party],
      na.rm = T
    ),
    mayor_party_vote = sum(
      vote[position == "vereador" & party == mayor_party],
      na.rm = T
    ),
    mayor_party_ideology_mean = mean(
      ideology[position == "vereador" & party == mayor_party],
      na.rm = T
    ),
    mayor_party_ideology_sd = sd(
      ideology[position == "vereador" & party == mayor_party],
      na.rm = T
    )
  ) %>%
  ungroup()

# mayor coalition vars
# note: coalition denotes that the vereador's party is present
# in mayor's electoral coalition
# note: warning signs displayed due to missing info for 80 candidates.
# choice: drop them
election <- election %>%
  group_by(
    election_year,
    cod_ibge_6
  ) %>%
  mutate(
    coalition_member = if_else(
      str_detect(mayor_coalition, paste0("\\b", party, "\\b")) & position == "vereador",
      1, 0
    ),
    coalition_share = sum(
      coalition_member == 1 & position == "vereador")/
      sum(position == "vereador"),
    coalition_margin = coalition_share - 0.5,
    coalition_majority = if_else(
      coalition_share > 0.5, 1, 0
    ),
    coalition_edu = mean(
      edu[position == "vereador" & coalition_member == 1],
      na.rm = T
    ),
    coalition_campaign = mean(
      campaign[position == "vereador" & coalition_member == 1],
      na.rm = T
    ),
    coalition_age = mean(
      age[position == "vereador" & coalition_member == 1],
      na.rm = T
    ),
    coalition_vote = sum(
      vote[position == "vereador" & coalition_member == 1],
      na.rm = T
    ),
    coalition_party_ideology_mean = mean(
      ideology[position == "vereador" & coalition_member == 1],
      na.rm = T
    ),
    coalition_party_ideology_sd = sd(
      ideology[position == "vereador" & coalition_member == 1],
      na.rm = T
    ),
    chamber_size = sum(position == "vereador")
  ) %>%
  ungroup()

# opposition structure
election <- election %>%
  group_by(
    election_year,
    cod_ibge_6
  ) %>%
  mutate(
    opposition_share = sum(
      coalition_member != 1 & position == "vereador")/
      sum(position == "vereador"),
    opposition_margin = opposition_share - 0.5,
    opposition_majority = if_else(
      opposition_share > 0.5, 1, 0
    ),
    opposition_edu = mean(
      edu[position == "vereador" & coalition_member != 1],
      na.rm = T
    ),
    opposition_campaign = mean(
      campaign[position == "vereador" & coalition_member != 1],
      na.rm = T
    ),
    opposition_age = mean(
      age[position == "vereador" & coalition_member != 1],
      na.rm = T
    ),
    opposition_vote = sum(
      vote[position == "vereador" & coalition_member != 1],
      na.rm = T
    ),
    opposition_party_ideology_mean = mean(
      ideology[position == "vereador" & coalition_member != 1],
      na.rm = T
    ),
    opposition_party_ideology_sd = sd(
      ideology[position == "vereador" & coalition_member != 1],
      na.rm = T
    )
  ) %>%
  ungroup()

# effective number of parties
election <- election %>%
  group_by(
    election_year,
    cod_ibge_6,
    party
  ) %>%
  mutate(
    party_share = sum(position == "vereador"),
    party_share = party_share/chamber_size,
    first_value = if_else(row_number() == 1, 1, 0)
  ) %>%
  ungroup() %>%
  group_by(
    election_year,
    cod_ibge_6
  ) %>%
  mutate(
    effective_parties = 1/sum(party_share[first_value == 1]^2)
  ) %>%
  ungroup() %>%
  select(
    -first_value,
    -party_share
  )

# retain elected mayors (hiring principals)
# subset
election <- election %>%
  filter(
    position == "prefeito"
  ) %>%
  mutate(
    cod_ibge_6 = as.character(cod_ibge_6)
  ) %>%
  select(
    cod_ibge_6,
    candidate_name,
    cpf_candidate,
    elec_title,
    election_year,
    mun_name,
    mayor_age = age,
    birthyear,
    mayor_edu = edu,
    mayor_male = gender,
    mayor_edu_desc = edu_desc,
    mayor_party = party,
    mayor_campaign = campaign,
    mayor_coalition = coalition,
    mayor_occupation = occupation,
    mayor_coalition_size = coalition_size,
    mayor_incumbent = incumbent,
    mayor_reelected = reelected,
    starts_with("mayor_party_"),
    starts_with("coalition"),
    starts_with("chamber_"),
    starts_with("party_"),
    starts_with("outcome"),
    contains("ideology"),
    starts_with("position"),
    total_vote_mayor,
    total_vote_legislative,
    effective_parties
  ) %>% 
  mutate(
    mayor_male = case_when(
      mayor_male == 2 ~ 1,
      mayor_male == 4 ~ 0,
      T ~ 2
    ),
    mayor_male = ifelse(
      mayor_male == 2, NA, mayor_male
    )
  )

# times in office
election <- election %>% 
  arrange(
    cpf_candidate,
    elec_title,
    candidate_name,
    election_year
  ) %>% 
  group_by(
    cpf_candidate,
    elec_title,
    candidate_name
  ) %>% 
  mutate(
    time_office = row_number() - 1
  ) %>% 
  ungroup()

# reorder dataframe
election <- election %>%
  arrange(
    cod_ibge_6,
    position_code,
    election_year
  ) %>% 
  select(
    order(colnames(.))
  ) %>%
  select(
    cod_ibge_6,
    election_year,
    everything()
  )

# write-out
election %>%
  fwrite(
    here("data/wrangle/election_chamber.csv")
  )

# coalition share for rdd -------------------------------------------------
# goal: calculate last minimal quotient for governing coalition and opposition
# elections
election <- list.files(
  here("data/wrangle"),
  pattern = "^election.csv",
  full.names = T
) %>%
  fread(
    nThread = parallel::detectCores() - 1,
    select = c(
      "cpf_candidate",
      "elec_title",
      "candidate_name",
      "cod_ibge_6",
      "cod_tse",
      "round",
      "mun_name",
      "election_year",
      "birthyear",
      "candidate_status",
      "party",
      "campaign",
      "edu",
      "age",
      "gender",
      "edu_desc",
      "coalition",
      "coalition_name",
      "position",
      "position_code",
      "position_previous",
      "occupation",
      "election_type",
      "outcome",
      "outcome_previous",
      "incumbent",
      "elected",
      "reelected",
      "ideology",
      "vote"
    )
  ) %>%
  filter(
    election_year %in% seq(2000, 2016, 4)
  )

# retain municipalities with only one type of election
# rule out problem of supplementary elections
election <- election %>%
  group_by(
    election_year,
    cod_ibge_6
  ) %>%
  mutate(
    n = n_distinct(election_type)
  ) %>%
  ungroup() %>% 
  filter(n == 1) %>%
  select(-n)

# extract municipalities with no second round
election <- election %>%
  group_by(
    cod_ibge_6,
    election_year
  ) %>%
  mutate(
    round_two = !any(round == 2)
  ) %>%
  ungroup() %>% 
  filter(
    round_two
  ) %>% 
  select(
    -starts_with("round")
  ) %>%
  ungroup

# eliminate defective entries with no elected officials
election <- election %>%
  group_by(
    cod_tse,
    election_type,
    position
  ) %>%
  filter(
    any(str_detect(outcome, "^eleito|media"))
  ) %>%
  ungroup()

# mayor coalition
# fix sao miguel do tapuio
# strangely there are some municipalities without mayor: 8 in total
# prob. because I eliminated supplementary election wins
# eliminate non elected municipalities
election <- election %>%
  filter(
    cod_ibge_6 != 221040
  ) %>%
  group_by(
    election_year,
    cod_ibge_6
  ) %>%
  filter(
    sum(position == "prefeito" & elected == 1, na.rm = T) == 1
  ) %>%
  mutate(
    mayor_party = party[position == "prefeito" & elected == 1],
    mayor_coalition = coalition[position == "prefeito" & elected == 1]
  ) %>%
  ungroup()

# fix coalitions
election %<>% 
  mutate(
    coalition = if_else(
      coalition == '',
      party,
      coalition
    )
  )

# mayor coalition vars
# note: coalition denotes that the vereador's party is present
# in mayor's electoral coalition
# note: warning signs displayed due to missing info for 80 candidates.
# choice: drop them
government <- election %>%
  filter(
    str_detect(mayor_coalition, paste0("\\b", party, "\\b")),
    position == "vereador"
  ) %>% 
  group_by(
    election_year,
    cod_ibge_6,
    coalition
  ) %>%
  summarise(
    coalition_n_candidates = n(),
    coalition_n_elected = sum(elected == 1),
    coalition_edu = mean(
      edu,
      na.rm = T
    ),
    coalition_campaign = mean(
      campaign,
      na.rm = T
    ),
    coalition_age = mean(
      age,
      na.rm = T
    ),
    coalition_vote = sum(
      vote,
      na.rm = T
    )
  ) %>%
  mutate(
    mayor_coalition_member = 1
  ) %>% 
  ungroup()

# opposition structure
opposition <- election %>%
  filter(
    !str_detect(mayor_coalition, paste0("\\b", party, "\\b")),
    position == "vereador"
  ) %>% 
  group_by(
    election_year,
    cod_ibge_6,
    coalition
  ) %>%
  summarise(
    coalition_n_candidates = n(),
    coalition_n_elected = sum(elected == 1),
    coalition_edu = mean(
      edu,
      na.rm = T
    ),
    coalition_campaign = mean(
      campaign,
      na.rm = T
    ),
    coalition_age = mean(
      age,
      na.rm = T
    ),
    coalition_vote = sum(
      vote,
      na.rm = T
    )
  ) %>%
  mutate(
    mayor_coalition_member = 0
  ) %>% 
  ungroup()

# chamber covariates
# note that one chamber, for one year, has 8 vereadores (min. is 9)
# I eliminate it
chamber <- election %>%
  filter(position == 'vereador') %>% 
  group_by(
    election_year,
    cod_ibge_6
  ) %>% 
  mutate(
    chamber_size = sum(position == 'vereador' & elected == 1)
  ) %>% 
  group_by(
    party,
    add = T
  ) %>%
  mutate(
    party_seat_share = sum(position == "vereador" & elected == 1),
    party_seat_share = party_seat_share/chamber_size,
    first_value = if_else(row_number() == 1, 1, 0)
  ) %>%
  group_by(
    election_year,
    cod_ibge_6
  ) %>%
  summarise(
    n_available_seats = sum(elected == 1),
    electoral_quotient = sum(vote)/mean(chamber_size),
    effective_parties = 1/sum(party_seat_share[first_value == 1]^2)
  ) %>%
  ungroup() %>%
  filter(n_available_seats >= 9)

# coalitions
coalition <- government %>% 
  bind_rows(opposition) %>% 
  left_join(
    chamber,
    by = c('election_year', 'cod_ibge_6')
  )

# create minimal quotient
quotient <- coalition %>% 
  filter(
    coalition_vote >= electoral_quotient
  ) %>% 
  mutate(round = map(n_available_seats, ~1:.x)) %>%
  unnest(round) %>% 
  select(
    cod_ibge_6,
    election_year,
    coalition,
    coalition_vote,
    mayor_coalition_member,
    n_available_seats,
    round
  )

quotient %<>% 
  mutate(
    electoral_quotient = coalition_vote/round
  ) %>%
  group_by(
    cod_ibge_6, election_year
  ) %>% 
  arrange(
    round,
    desc(electoral_quotient)
  ) %>% 
  slice(
    1:(unique(n_available_seats) + 1)
  ) %>%
  ungroup()

# keep municipalities where seats flip across coaliitons (gov and opposition)
quotient %<>%
  group_by(
    cod_ibge_6, 
    election_year
  ) %>% 
  mutate(
    last_seat = if_else(row_number() == n() - 1, 1, 0),
    runner_up = if_else(row_number() == n(), 1, 0),
    marginal = last_seat + runner_up
  ) %>% 
  ungroup()

# running variable
quotient %<>% 
  group_by(
    cod_ibge_6, election_year
  ) %>% 
  summarise(
    mayor_opposition_flip = if_else(mayor_coalition_member[last_seat == 1] != mayor_coalition_member[runner_up == 1], 1, 0),
    quotient_margin = electoral_quotient[last_seat == 1] - electoral_quotient[runner_up == 1],
    total_vote = sum(coalition_vote),
    quotient_margin = quotient_margin/total_vote
  ) %>% 
  ungroup()

# write-out
quotient %>%
  fwrite(
    here('data/wrangle/election_coalition.csv')
  )

# details -----------------------------------------------------------------
# details denotes the tally performed by the tse post election
# count total votes cast as well as absentee/blank votes
details <- list.files(
  here("data/raw/election"),
  "^details",
  full.names = T
) %>% 
  map_dfr(
    . %>%
      gunzip(
        remove = F,
        temporary = T,
        overwrite = T
      ) %>% 
      fread(
        nThread = parallel::detectCores() - 1,
        encoding = "Latin-1",
        colClasses = c("SIGLA_UE" = "character")
      )
  )

# fix cols
details <- details %>% 
  transmute(
    cod_tse = str_pad(CODIGO_MUNICIPIO, 5, "left", 0),
    election_year = ANO_ELEICAO,
    state = SIGLA_UF,
    election_type = DESCRICAO_ELEICAO,
    round = NUM_TURNO,
    position_code = CODIGO_CARGO,
    position = DESCRICAO_CARGO,
    registered_voters = QTD_APTOS,
    vote_turnout = QTD_COMPARECIMENTO,
    vote_abstention = QTD_ABSTENCOES,
    vote_nominal = QTD_VOTOS_NOMINAIS,
    vote_blank = QTD_VOTOS_BRANCOS + QTD_VOTOS_NULOS,
    vote_legend = QTD_VOTOS_LEGENDA,
    vote_cancelled = QTD_VOTOS_ANULADOS
  )

# remove special characters
details <- details %>% 
  mutate_if(
    is.character,
    str_to_lower
  ) %>% 
  mutate_if(
    is.character,
    funs(
      stri_trans_general(., "latin-ascii")
    )
  )

# drop foreign voting and transit vote
# lose 1454 obs.
details <- details %>% 
  filter(
    state != "zz",
    state != "spool off",
    state != "",
    state != "vt"
  )

# fix cod_tse and append cod_ibge_6
identifiers <- fread(
  here("files/cod_ibge_tse.csv"),
  colClasses = c("cod_tse" = "character", "cod_ibge_7" = "character")
) %>% 
  transmute(
    state,
    cod_tse,
    cod_ibge_6 = str_sub(cod_ibge_7, 1, 6),
    mun_name = mun_name_tse
  )

# append and fix cod_tse
details <- details %>% 
  left_join(
    identifiers %>% 
      select(
        cod_tse,
        cod_ibge_6
      ),
    by = c("cod_tse")
  )

# aggregate by municipality
group_var <- names(details)[!str_detect(names(details), "^vote|voters$")]

details <- details %>% 
  group_by_at(
    vars(one_of(group_var))
  ) %>% 
  summarise_all(
    sum, na.rm = T
  ) %>% 
  ungroup()

# fix defective turnout and registered voters
# entries are underreporting both columsn (vote_turnout, registered_voters)
details <- details %>% 
  mutate(
    vote_turnout = vote_nominal ~ vote_nominal + vote_blank + vote_legend + vote_cancelled,
    registered_voters = vote_turnout + vote_abstention
  )

# note that there are still 700 defective entries. I have chosen to add the micro-level data and fix it accordingly.
# that is 0.5% of total observations in the data.
# electoral data
election <- list.files(
  "~/princeton/R/data/tse/data/wrangle",
  pattern = "^election.csv.gz$",
  full.names = T
) %>% 
  fread(
    nThread = parallel::detectCores() - 1
  )

# select vars
election <- election %>% 
  transmute(
    cod_ibge_6,
    state,
    party,
    round,
    position,
    election_year,
    election_type,
    cpf_candidate,
    candidate_name,
    coalition,
    age = election_year - as.integer(birthyear),
    candidate_status,
    state,
    outcome,
    vote
  )

# blank votes
vote <- list.files(
  "~/princeton/R/data/tse/data/wrangle/",
  "vote_count",
  full.names = T
) %>% 
  fread

# vote count election
election_mun <- election %>% 
  group_by(
    cod_ibge_6,
    election_year,
    election_type,
    position,
    round
  ) %>% 
  summarise(
    vote_nominal = sum(vote)
  ) %>% 
  ungroup()

details <- election_mun %>% 
  select(
    cod_ibge_6,
    election_year,
    election_type,
    position,
    round,
    vote_nominal
  ) %>% 
  right_join(
    details %>% 
      mutate(
        cod_ibge_6 = as.integer(cod_ibge_6)
      ), 
    by = c("cod_ibge_6", "election_year", "round", "position", "election_type")
  )

# fix entries with micro-level data
details <- details %>% 
  mutate(
    vote_turnout = case_when(
      vote_nominal.x >= vote_nominal.y ~ vote_nominal.x + vote_blank + vote_legend + vote_cancelled,
      T ~ vote_nominal.y
    ),
    registered_voters = case_when(
      vote_nominal.x >= vote_nominal.y ~ vote_turnout + vote_abstention,
      T ~ vote_nominal.y
    ),
    vote_nominal = case_when(
      vote_nominal.x >= vote_nominal.y ~ vote_nominal.x,
      T ~ vote_nominal.y
    )
  ) %>% 
  select(
    -ends_with(".x"),
    -ends_with(".y")
  )

# rearange
details <- details %>% 
  select(
    order(colnames(.))
  ) %>% 
  select(
    cod_tse,
    cod_ibge_6,
    election_year,
    everything()
  ) %>% 
  arrange(
    cod_tse,
    cod_ibge_6,
    election_year
  )

# write-out
details %>% 
  fwrite(
    here("data/wrangle/vote_count.csv")
  )

# still one observation left unmatched. 1 vereador in cod_ibge_6 = 240450, election_year = 2002.
# ignore it.
