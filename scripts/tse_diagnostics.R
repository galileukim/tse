# set-up ------------------------------------------------------------------
lapply(
  c(
    "tidyverse",
    "R.utils",
    "data.table",
    "stringi",
    "lubridate",
    "electionsBR"
  ),
  require,
  character.only = T
)

set.seed(1789)

# read-in -----------------------------------------------------------------
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

# validation --------------------------------------------------------------
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

election_mun <- election_mun %>% 
  left_join(
    details %>% 
      mutate(
        cod_ibge_6 = as.integer(cod_ibge_6)
      ) %>% 
      select(
        cod_ibge_6,
        election_year,
        position,
        round,
        registered_voters,
        election_type,
        starts_with("vote")
      ) %>% 
      rename(
        vote_nominal_2 = vote_nominal
      ), 
    by = c("cod_ibge_6", "election_year", "round", "position", "election_type")
  )

vote_mun <- vote %>% 
  group_by(
    cod_ibge_6,
    position,
    election_year
  ) %>% 
  summarise(
    vote_nominal = sum(vote_nominal),
    vote_turnout = sum(vote_turnout)
  ) %>% 
  ungroup()
