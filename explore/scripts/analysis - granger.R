



library(finRes); library(magrittr); library(doParallel)
source("explore/scripts/functions - shared.r")

# start cluster ####
cluster <- makeCluster(detectCores() - 1L); registerDoParallel(cluster)
# globals ####
data("tickers_futures", "tickers_cftc", package = "BBGsymbols"); data("exchanges", package = "fewISOs")
storethat <- "/media/storage/Dropbox/code/R/Projects/thesis/data/storethat.sqlite"
periods <- tibble::tibble(period = c(rep("past", 2L), rep("financialization", 2L)), bound = rep(c("start", "end"), 2L),
                          date = c("1997-07-01", "2003-12-31", "2004-01-01", "2008-09-14"))
fields <- tibble::tibble(name = forcats::as_factor(c("Open", "High", "Low", "Close", "OI", "volume")), 
                         symbol = c("PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST", "FUT_AGGTE_OPEN_INT", "FUT_AGGTE_VOL"))
widths <- tibble::tibble(frequency = c("day", "week", "month"), width = c(252L, 52L, 12L))
orders <- tibble::tibble(frequency = c("day", "week", "month"), order = c(252L, 52L, 12L))
# data ####
## loading ####
start <- "1996-01-01"; end <- "2008-09-14"
`commodity futures tickers` <- c("KWA Comdty", "W A Comdty", "C A Comdty", "S A Comdty")
`commodity term structure data` <- pullit::pull_futures_market(source = "storethat", type = "term structure", active_contract_tickers = `commodity futures tickers`,
                                                               start = start, end = end, TS_positions = 1L, roll_type = "A", roll_days = 0L, roll_months = 0L,
                                                               roll_adjustment = "N", file = storethat)
`commodity aggregate data` <- pullit::pull_futures_market(source = "storethat", type = "aggregate", active_contract_tickers = `commodity futures tickers`,
                                                          start = start, end = end, file = storethat)
`commodity spot data` <- pullit::pull_futures_market(source = "storethat", type = "spot", active_contract_tickers = `commodity futures tickers`,
                                                     start = start, end = end, file = storethat)
`commodity CFTC data` <- pullit::pull_futures_CFTC(source = "storethat", active_contract_tickers = `commodity futures tickers`, start = start, end = end, file = storethat)
CFTC <- dplyr::left_join(`commodity CFTC data`@data, dplyr::select(tickers_cftc, -c(name, `active contract ticker`)), by = "ticker") %>% 
  dplyr::filter(format == "legacy", underlying == "futures only", unit == "contracts", participant %in% c("commercial", "non-commercial", "total"), 
                position %in% c("long", "short", "net")) %>%  dplyr::select(`active contract ticker`, participant, position, date, value) %>% 
  dplyr::mutate(date = RQuantLib::advance(dates = date, n = 3L, timeUnit = 0L, calendar = "UnitedStates/NYSE"))
## munging ####
`term structure` <- dplyr::left_join(`commodity term structure data`@data, `commodity term structure data`@term_structure_tickers, by = "ticker") %>% 
  dplyr::select(ticker = `active contract ticker`, field, date, value) %>% dplyr::filter(field %in% c("PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST")) %>% 
  dplyr::mutate(variable = "futures price")
spot <- dplyr::filter(`commodity spot data`@data, field %in% c("PX_HIGH", "PX_LOW", "PX_LAST")) %>% dplyr::mutate(variable = "spot price") 
basis <- dplyr::bind_rows(dplyr::filter(`term structure`, field %in% c("PX_HIGH", "PX_LOW", "PX_LAST")), spot) %>% dplyr::group_by(ticker, field) %>% 
  tidyr::spread(variable, value) %>% dplyr::mutate(basis = `futures price` - `spot price`) %>% dplyr::select(ticker, field, date, basis) %>% 
  tidyr::gather(variable, value, -c(ticker, field, date)) %>% dplyr::ungroup()
`commodity futures data` <- dplyr::bind_rows(`term structure`, basis) %>% dplyr::left_join(fields, by = c("field" = "symbol")) %>% 
  dplyr::select(ticker, variable, field = name, date, value) %>% dplyr::arrange(ticker, variable, field, date)

# market variables ####
width <- dplyr::filter(widths, frequency == "day") %>% dplyr::select(width) %>% purrr::flatten_int()
order <- dplyr::filter(orders, frequency == "day") %>% dplyr::select(order) %>% purrr::flatten_int()
## volatilities ####
futures <- tibble::tibble(variable = rep("futures price", 6L), estimator = c("close", "garman.klass", "parkinson", "rogers.satchell", "gk.yz", "yang.zhang"))
basis <- tibble::tibble(variable = rep("basis", 2L), estimator = c("close", "parkinson"))
estimators <- dplyr::bind_rows(futures, basis)
combinations <- lapply(unique(`commodity futures data`$ticker), function(x) dplyr::mutate(estimators, commodity = x)) %>% dplyr::bind_rows() %>% as.data.frame()
volatilities <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr)
  
  ticker <- combinations[y, "commodity"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]
  
  data <- dplyr::filter(`commodity futures data`, ticker == !! ticker, variable == !! variable) %>% dplyr::select(field, date, value) %>% 
    dplyr::arrange(field, date) %>% tidyr::spread(field, value) %>% dplyr::filter(complete.cases(.))
  if (variable == "basis"){
    data <- switch(estimator, "parkinson" = dplyr::mutate(data, Open = NA, Close = NA) %>% dplyr::select(date, Open, High, Low, Close),
                   "close" = dplyr::select(data, date, Close), data)
  }
  
  dplyr::mutate(data, volatility = TTR::volatility(dplyr::select(data, -date), n = width, calc = estimator, N = width)) %>%
    dplyr::filter(! is.na(volatility)) %>% dplyr::mutate(estimator = !! estimator, ticker = !! ticker, variable = !! variable) %>% 
    dplyr::select(ticker, variable, estimator, date, volatility)
}
## munging ####
contemporaneous <- dplyr::left_join(`commodity aggregate data`@data, fields, by = c("field" = "symbol")) %>% dplyr::select(ticker, field = name, date, value) %>%
  dplyr::mutate_at(dplyr::vars(field), dplyr::funs(as.character))
combinations <- dplyr::distinct(contemporaneous, ticker, field)
lagged <- lapply(1L:nrow(combinations), function(x) { ticker <- combinations[x, "ticker"]; field <- combinations[x, "field"]
dplyr::filter(contemporaneous, ticker == !! ticker, field == !! field) %>% dplyr::mutate(value = dplyr::lag(value, 1L), field = paste("lagged", field))
}) %>% dplyr::bind_rows()
regressors <- dplyr::bind_rows(contemporaneous, lagged) %>% dplyr::arrange(ticker, field, date)
## granger ####
combinations <- expand.grid.df(dplyr::distinct(volatilities, ticker, variable, estimator), 
                               dplyr::filter(regressors, ticker %in% unique(volatilities$ticker)) %>% dplyr::select(-ticker) %>% dplyr::distinct(regressor = field), 
                               dplyr::distinct(periods, period)) %>% as.data.frame()
# combinations <- dplyr::slice(combinations, 1L:20L)
`market variables` <- foreach(y = 1L:nrow(combinations), .combine = rbind) %dopar% {
  library(magrittr)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]; period <- combinations[y, "period"]
  estimator <- combinations[y, "estimator"]; regressor <- combinations[y, "regressor"]
  
  start <- dplyr::filter(periods, period == !! period, bound == "start") %>% dplyr::select(date) %>% purrr::flatten_chr()
  end <- dplyr::filter(periods, period == !! period, bound == "end") %>% dplyr::select(date) %>% purrr::flatten_chr()
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator, date >= as.Date(start), date <= as.Date(end))
  variable <- dplyr::filter(regressors, ticker == !! ticker, field == !! regressor, date >= as.Date(start), date <= as.Date(end)) %>% 
    tidyr::spread(field, value) %>% dplyr::select(-ticker)
  data <- dplyr::left_join(volatility, variable, by = "date") %>% dplyr::mutate(regressor = !! regressor) %>% 
    dplyr::select(ticker, variable, estimator, regressor, date, dplyr::everything())
  
  formula <- as.formula(paste0("volatility ~ `", regressor, "`"))
  granger <- tryCatch({ lmtest::grangertest(formula, order = order, data = data) }, error = function(e) { NA })
  `leg 1` <- tibble::tibble(frequency = "day", period = eval(period), ticker = eval(ticker), variable = combinations[y, "variable"], estimator = eval(estimator),
                            Y = "volatility", X = eval(regressor), data = list(data), model = list(granger))
  formula <- as.formula(paste0("`", regressor, "` ~ volatility"))
  granger <- tryCatch({ lmtest::grangertest(formula, order = order, data = data) }, error = function(e) { NA })
  `leg 2` <- tibble::tibble(frequency = "day", period = eval(period), ticker = eval(ticker), variable = combinations[y, "variable"], estimator = eval(estimator), 
                            Y = eval(regressor), X = "volatility", data = list(data), model = list(granger))
  
  dplyr::bind_rows(`leg 1`, `leg 2`)
}



# CFTC variables ####
width <- dplyr::filter(widths, frequency == "week") %>% dplyr::select(width) %>% purrr::flatten_int()
order <- dplyr::filter(orders, frequency == "week") %>% dplyr::select(order) %>% purrr::flatten_int()
## contemporaneous ####
### regressors ####
Working <- dplyr::filter(CFTC, participant == "commercial", position %in% c("long", "short")) %>% dplyr::group_by(`active contract ticker`) %>% 
  tidyr::spread(position, value) %>% dplyr::mutate(regime = ifelse(short > long, 1L, 0L), total = long + short) %>% 
  dplyr::select(`active contract ticker`, date, regime, total) %>% dplyr::ungroup()
Working <- dplyr::filter(CFTC, participant == "non-commercial", position %in% c("long", "short")) %>% dplyr::select(-participant) %>%
  dplyr::left_join(Working, by = c("active contract ticker", "date")) %>% dplyr::group_by(`active contract ticker`) %>% 
  tidyr::spread(position, value) %>% dplyr::mutate(`Working's T` = (ifelse(regime == 1L, short, long) / total)) %>%
  dplyr::select(`active contract ticker`, date, `Working's T`) %>% tidyr::gather(field, value, -c(`active contract ticker`, date)) %>%
  dplyr::ungroup()
regressors <- dplyr::bind_rows(dplyr::filter(CFTC, position == "net") %>% dplyr::select(`active contract ticker`, field = participant, date, value), Working) %>%
  dplyr::rename(ticker = `active contract ticker`)
contemporaneous <- dplyr::left_join(dplyr::filter(`commodity aggregate data`@data, field == "FUT_AGGTE_VOL"), fields, by = c("field" = "symbol")) %>% 
  dplyr::select(ticker, field = name, date, value) %>% dplyr::mutate_at(dplyr::vars(field), dplyr::funs(as.character)) %>% dplyr::filter(date %in% unique(regressors$date))
regressors <- dplyr::bind_rows(regressors, contemporaneous) %>% dplyr::arrange(ticker, field, date)
### volatilities ####
futures <- tibble::tibble(variable = rep("futures price", 6L), estimator = c("close", "garman.klass", "parkinson", "rogers.satchell", "gk.yz", "yang.zhang"))
basis <- tibble::tibble(variable = rep("basis", 2L), estimator = c("close", "parkinson"))
estimators <- dplyr::bind_rows(futures, basis)
combinations <- lapply(unique(`commodity futures data`$ticker), function(x) dplyr::mutate(estimators, commodity = x)) %>% dplyr::bind_rows() %>% as.data.frame()
volatilities <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  
  ticker <- combinations[y, "commodity"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]
  
  data <- dplyr::filter(`commodity futures data`, ticker == !! ticker, variable == !! variable, date %in% unique(regressors$date)) %>% 
    dplyr::select(field, date, value) %>% dplyr::arrange(field, date) %>% tidyr::spread(field, value) %>% dplyr::filter(complete.cases(.))
  if (variable == "basis"){
    data <- switch(estimator, "parkinson" = dplyr::mutate(data, Open = NA, Close = NA) %>% dplyr::select(date, Open, High, Low, Close),
                   "close" = dplyr::select(data, date, Close), data)
  }
  
  dplyr::mutate(data, volatility = TTR::volatility(dplyr::select(data, -date), n = width, calc = estimator, N = width)) %>%
    dplyr::filter(! is.na(volatility)) %>% dplyr::mutate(estimator = !! estimator, ticker = !! ticker, variable = !! variable) %>% 
    dplyr::select(ticker, variable, estimator, date, volatility)
}
### granger ####
combinations <- expand.grid.df(dplyr::distinct(volatilities, ticker, variable, estimator), 
                               dplyr::filter(regressors, ticker %in% unique(volatilities$ticker)) %>% dplyr::select(-ticker) %>% dplyr::distinct(regressor = field), 
                               dplyr::distinct(periods, period)) %>% as.data.frame()
`contemporaneous results` <- foreach(y = 1L:nrow(combinations), .combine = rbind) %dopar% {
  library(magrittr)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]; period <- combinations[y, "period"]
  estimator <- combinations[y, "estimator"]; regressor <- combinations[y, "regressor"]
  
  start <- dplyr::filter(periods, period == !! period, bound == "start") %>% dplyr::select(date) %>% purrr::flatten_chr()
  end <- dplyr::filter(periods, period == !! period, bound == "end") %>% dplyr::select(date) %>% purrr::flatten_chr()
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator, date >= as.Date(start), date <= as.Date(end))
  variable <- dplyr::filter(regressors, ticker == !! ticker, field == !! regressor, date >= as.Date(start), date <= as.Date(end)) %>% 
    tidyr::spread(field, value) %>% dplyr::select(-ticker)
  data <- dplyr::left_join(volatility, variable, by = "date") %>% dplyr::mutate(regressor = !! regressor) %>% 
    dplyr::select(ticker, variable, estimator, regressor, date, dplyr::everything())
  
  formula <- as.formula(paste0("volatility ~ `", regressor, "`"))
  granger <- tryCatch({ lmtest::grangertest(formula, order = order, data = data) }, error = function(e) { NA })
  `leg 1` <- tibble::tibble(frequency = "week", period = eval(period), ticker = eval(ticker), variable = combinations[y, "variable"], estimator = eval(estimator),
                            Y = "volatility", X = eval(regressor), data = list(data), model = list(granger))
  formula <- as.formula(paste0("`", regressor, "` ~ volatility"))
  granger <- tryCatch({ lmtest::grangertest(formula, order = order, data = data) }, error = function(e) { NA })
  `leg 2` <- tibble::tibble(frequency = "week", period = eval(period), ticker = eval(ticker), variable = combinations[y, "variable"], estimator = eval(estimator), 
                            Y = eval(regressor), X = "volatility", data = list(data), model = list(granger))
  
  dplyr::bind_rows(`leg 1`, `leg 2`)
}
## lagged ####
### regressors ####
Working <- dplyr::filter(CFTC, participant == "commercial", position %in% c("long", "short")) %>% dplyr::group_by(`active contract ticker`) %>% 
  tidyr::spread(position, value) %>% dplyr::mutate(regime = ifelse(short > long, 1L, 0L), total = long + short) %>% 
  dplyr::select(`active contract ticker`, date, regime, total) %>% dplyr::ungroup()
Working <- dplyr::filter(CFTC, participant == "non-commercial", position %in% c("long", "short")) %>% dplyr::select(-participant) %>%
  dplyr::left_join(Working, by = c("active contract ticker", "date")) %>% dplyr::group_by(`active contract ticker`) %>% 
  tidyr::spread(position, value) %>% dplyr::mutate(`Working's T` = (ifelse(regime == 1L, short, long) / total)) %>%
  dplyr::select(`active contract ticker`, date, `Working's T`) %>% tidyr::gather(field, value, -c(`active contract ticker`, date)) %>%
  dplyr::ungroup()
regressors <- dplyr::bind_rows(dplyr::filter(CFTC, position == "net") %>% dplyr::select(`active contract ticker`, field = participant, date, value), Working) %>%
  dplyr::rename(ticker = `active contract ticker`)
contemporaneous <- dplyr::left_join(dplyr::filter(`commodity aggregate data`@data, field == "FUT_AGGTE_VOL"), fields, by = c("field" = "symbol")) %>% 
  dplyr::select(ticker, field = name, date, value) %>% dplyr::mutate_at(dplyr::vars(field), dplyr::funs(as.character)) %>% dplyr::filter(date %in% unique(regressors$date))
regressors <- dplyr::bind_rows(regressors, contemporaneous) %>% dplyr::arrange(ticker, field, date)
regressors <- dplyr::group_by(regressors, ticker, field) %>% dplyr::mutate(value = dplyr::lag(value, 1L)) %>% dplyr::ungroup() %>%
  dplyr::group_by(ticker) %>% dplyr::mutate(field = paste("lagged", field)) %>% dplyr::ungroup()
### volatilities ####
futures <- tibble::tibble(variable = rep("futures price", 6L), estimator = c("close", "garman.klass", "parkinson", "rogers.satchell", "gk.yz", "yang.zhang"))
basis <- tibble::tibble(variable = rep("basis", 2L), estimator = c("close", "parkinson"))
estimators <- dplyr::bind_rows(futures, basis)
combinations <- lapply(unique(`commodity futures data`$ticker), function(x) dplyr::mutate(estimators, commodity = x)) %>% dplyr::bind_rows() %>% as.data.frame()
volatilities <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  
  ticker <- combinations[y, "commodity"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]
  
  data <- dplyr::filter(`commodity futures data`, ticker == !! ticker, variable == !! variable, date %in% unique(regressors$date)) %>% 
    dplyr::select(field, date, value) %>% dplyr::arrange(field, date) %>% tidyr::spread(field, value) %>% dplyr::filter(complete.cases(.))
  if (variable == "basis"){
    data <- switch(estimator, "parkinson" = dplyr::mutate(data, Open = NA, Close = NA) %>% dplyr::select(date, Open, High, Low, Close),
                   "close" = dplyr::select(data, date, Close), data)
  }
  
  dplyr::mutate(data, volatility = TTR::volatility(dplyr::select(data, -date), n = width, calc = estimator, N = width)) %>%
    dplyr::filter(! is.na(volatility)) %>% dplyr::mutate(estimator = !! estimator, ticker = !! ticker, variable = !! variable) %>% 
    dplyr::select(ticker, variable, estimator, date, volatility)
}
### granger ####
combinations <- expand.grid.df(dplyr::distinct(volatilities, ticker, variable, estimator), 
                               dplyr::filter(regressors, ticker %in% unique(volatilities$ticker)) %>% dplyr::select(-ticker) %>% dplyr::distinct(regressor = field), 
                               dplyr::distinct(periods, period)) %>% as.data.frame()
`lagged results` <- foreach(y = 1L:nrow(combinations), .combine = rbind) %dopar% {
  library(magrittr)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]; period <- combinations[y, "period"]
  estimator <- combinations[y, "estimator"]; regressor <- combinations[y, "regressor"]
  
  start <- dplyr::filter(periods, period == !! period, bound == "start") %>% dplyr::select(date) %>% purrr::flatten_chr()
  end <- dplyr::filter(periods, period == !! period, bound == "end") %>% dplyr::select(date) %>% purrr::flatten_chr()
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator, date >= as.Date(start), date <= as.Date(end))
  variable <- dplyr::filter(regressors, ticker == !! ticker, field == !! regressor, date >= as.Date(start), date <= as.Date(end)) %>% 
    tidyr::spread(field, value) %>% dplyr::select(-ticker)
  data <- dplyr::left_join(volatility, variable, by = "date") %>% dplyr::mutate(regressor = !! regressor) %>% 
    dplyr::select(ticker, variable, estimator, regressor, date, dplyr::everything())
  
  formula <- as.formula(paste0("volatility ~ `", regressor, "`"))
  granger <- tryCatch({ lmtest::grangertest(formula, order = order, data = data) }, error = function(e) { NA })
  `leg 1` <- tibble::tibble(frequency = "week", period = eval(period), ticker = eval(ticker), variable = combinations[y, "variable"], estimator = eval(estimator),
                            Y = "volatility", X = eval(regressor), data = list(data), model = list(granger))
  formula <- as.formula(paste0("`", regressor, "` ~ volatility"))
  granger <- tryCatch({ lmtest::grangertest(formula, order = order, data = data) }, error = function(e) { NA })
  `leg 2` <- tibble::tibble(frequency = "week", period = eval(period), ticker = eval(ticker), variable = combinations[y, "variable"], estimator = eval(estimator), 
                            Y = eval(regressor), X = "volatility", data = list(data), model = list(granger))
  
  dplyr::bind_rows(`leg 1`, `leg 2`)
}


`CFTC variables` <- dplyr::bind_rows(`contemporaneous results`, `lagged results`)
granger <- tibble::tibble(analysis = c("market variables", "CFTC variables"), results = list(`market variables`, `CFTC variables`))

parallel::stopCluster(cluster); gc()
saveRDS(granger, file = "explore/results/granger.rds")

