library(rstan)
library(data.table)
library(lubridate)
library(gdata)
library(EnvStats)

# Rscript base.r us_base 150 4000 
args = commandArgs(trailingOnly=TRUE)
StanModel = args[1]
minimumReportedDeaths = as.integer(args[2])
nStanIterations = as.integer(args[3])
print(sprintf("Running stan model %s",StanModel))
print(sprintf("Only running on counties with at least %d total reported deaths", minimumReportedDeaths))
print(sprintf("Running MCMC routine with %d iterations", nStanIterations))

# case-mortality table
d <- read.csv("../modelInput/ILCaseAndMortalityV1.csv")

###
# HERE! -> specify a date through which to run the model
# i.e., specify day of last observation
# say it's June 8th, but we only want to run through June 1st, because the model hasn't been adjusted for lifting lockdown yet
# want to easily do this -> can easily do this -> just select columns with date <= target date
# something a la:
# validationObs <- countyObs[as.Date(countyObs$dateRep, format = "%m/%d/%y") > lastObs, ]
# dateCutoff <- "6/1/20"
# dateCutoff <- as.Date(dateCutoff, format = "%m/%d/%y")
# print(sprintf("date cutoff: %s",  dateCutoff))
# d <- d[as.Date(d$dateRep, format = "%m/%d/%y") <= lastObs, ]
###

d$countryterritoryCode <- sapply(d$countryterritoryCode, as.character)

# drop counties with fewer than cutoff cumulative deaths or cases
cumCaseAndDeath <- aggregate(cbind(d$deaths), by=list(Category=d$countryterritoryCode), FUN=sum)

dropCounties <- subset(cumCaseAndDeath, V1 < minimumReportedDeaths)$Category
d <- subset(d, !(countryterritoryCode %in% dropCounties))
# print(sprintf("nCounties with more than %d deaths before %s: %d", minimumReportedDeaths, dateCutoff, length(unique(d$countryterritoryCode))))
print(sprintf("nCounties with more than %d deaths: %d", minimumReportedDeaths, length(unique(d$countryterritoryCode))))

# 84017031 -> ID for Cook County
# 84017043 -> ID for DuPage County
# HERE -> testing running the model, bigger simulation, just for Cook and DuPage counties
# comment this line out to run the sim for all IL counties
# d <- subset(d, countryterritoryCode %in% list("84017031", "84017043"))

countries <- unique(d$countryterritoryCode)

# weighted fatality table
cfr.by.country = read.csv("../modelInput/ILWeightedFatalityV1.csv")
cfr.by.country$country = as.character(cfr.by.country[,3])

# serial interval discrete gamma distribution
serial.interval = read.csv("../modelInput/ILSerialIntervalV1.csv") # new table

# interventions table 
# NOTE: "covariate" == "intervention"; 
# e.g., if there are 3 different interventions in the model, then there are 3 covariates here in the code
covariates = read.csv("../modelInput/ILInterventionsV1.csv", stringsAsFactors = FALSE)
covariates$Country <- sapply(covariates$Country, as.character)
p <- ncol(covariates) - 2
forecast = 0

# NOTE: 7 (6?) is the NUMBER OF COVARIATES in the original model -> see comment "icl: models should only take 6 covariates"
# --> a hardcoded seven represents the number of covariates (i.e., interventions)
# --> should be dynamic, just computed from the table of interventions

# icl: Increase this for a further forecast
# N2 = 75 
# err if N2 is less than number of days of data for a given cluster -> e.g., Chicago has ~90, threw err for N2 == 75
# see: N2 before correction - also serial interval note
N2 = 75 # changed -> fixed the error; so when N2 has to get updated, the script fails
dates = list()
reported_cases = list()
deaths_by_country = list()

stan_data = list(M=length(countries),
                N=NULL,
                p=p,
                # x1=poly(1:N2,2)[,1], # N2 before correction -> causes errors
                # x2=poly(1:N2,2)[,2], # N2 before correction
                y=NULL,
                covariate1=NULL, # -> lockdown -> presently the only intervention in the IL model 
                deaths=NULL,
                f=NULL,
                N0=6, # icl: N0 = 6 to make it consistent with Rayleigh # ? td: check this
                cases=NULL,
                LENGTHSCALE=p, # this is the number of covariates (i.e., the number of interventions)
                # SI=serial.interval$fit[1:N2], # N2 before correction
                EpidemicStart = NULL)

# new - adjust N2 before main procesesing routine - i.e., adjust N2 so that it's uniform across all counties
# fixme: can definitely make this more efficient, or at least wrap it into a function
# note: N2 is the length of time window to simulate - must be the same across counties
for(Country in countries) {

  tmp=d[d$countryterritoryCode==Country,]
  tmp$date = as.Date(tmp$dateRep,format='%m/%d/%y')
  tmp$t = decimal_date(tmp$date) 
  tmp=tmp[order(tmp$t),]

  index1 = which(cumsum(tmp$deaths)>=10)[1] 
  index2 = index1-30
  
  tmp=tmp[index2:nrow(tmp),]
  
  N = length(tmp$cases)  
  if(N2 - N < 0) {
    print(sprintf("raising N2 from %d to %d", N2, N))
    # testing..
    # N2 = N
    N2 = N + 7
  }
}

# HERE! -> refactor, remove comments

print(sprintf("uniform N2: %d", N2))

for(Country in countries) {

  CFR=cfr.by.country$weighted_fatality[cfr.by.country$country == Country]

  covariates1 <- covariates[covariates$Country == Country, 3:ncol(covariates), drop=FALSE]

  d1=d[d$countryterritoryCode==Country,]

  d1$date = as.Date(d1$dateRep,format='%m/%d/%y')

  d1$t = decimal_date(d1$date) 
  d1=d1[order(d1$t),]

  index = which(d1$cases>0)[1]
  index1 = which(cumsum(d1$deaths)>=10)[1] 
  index2 = index1-30
  
  print(sprintf("First non-zero cases is on day %d, and 30 days before 10 cumulative deaths is day %d",index,index2))
  d1=d1[index2:nrow(d1),]
  stan_data$EpidemicStart = c(stan_data$EpidemicStart,index1+1-index2)
  
  for (ii in 1:ncol(covariates1)) {
    covariate = names(covariates1)[ii]
    
    # 0 -> 1 (lockdown doesn't end)
    d1[covariate] <- (as.Date(d1$dateRep, format='%m/%d/%y') >= as.Date(covariates1[1,covariate]))*1  # icl: should this be > or >=?
  }

  # dates[[as.character(Country)]] = d1$date
  dates[[Country]] = d1$date

  # hazard estimation
  N = length(d1$cases)
  print(sprintf("%s has %d days of data",Country,N))
  
  # at least a seven day forecast
  # testing this
  forecast <- max(N2 - N, 7)
  
  h = rep(0,forecast+N) # discrete hazard rate from time t = 1, ..., 100
  mean1 = 5.1; cv1 = 0.86; # infection to onset
  mean2 = 18.8; cv2 = 0.45 # onset to death

  # td: double check these comments
  # otherwise should be fine
  ## icl: assume that CFR is probability of dying given infection
  x1 = rgammaAlt(5e6,mean1,cv1) # icl: infection-to-onset ----> do all people who are infected get to onset?
  x2 = rgammaAlt(5e6,mean2,cv2) # icl: onset-to-death
  f = ecdf(x1+x2)
  convolution = function(u) (CFR * f(u))
  h[1] = (convolution(1.5) - convolution(0)) 
  for(i in 2:length(h)) {
    h[i] = (convolution(i+.5) - convolution(i-.5)) / (1-convolution(i-.5))
  }
  s = rep(0,N2)
  s[1] = 1 
  for(i in 2:N2) {
    s[i] = s[i-1]*(1-h[i-1])
  }
  f = s * h
  
  # looks like 'y' may be the problem 
  y=c(as.vector(as.numeric(d1$cases)),rep(-1,forecast))
  reported_cases[[Country]] = as.vector(as.numeric(d1$cases))
  deaths=c(as.vector(as.numeric(d1$deaths)),rep(-1,forecast))
  cases=c(as.vector(as.numeric(d1$cases)),rep(-1,forecast))
  deaths_by_country[[Country]] = as.vector(as.numeric(d1$deaths))
  covariates2 <- as.data.frame(d1[, colnames(covariates1)])
  covariates2[N:(N+forecast),] <- covariates2[N,]
  
  # print(sprintf("N: %d", N))
  # print(sprintf("N2: %d", N2))
  # print(sprintf("length(cases): %d", length(cases)))
  # print(sprintf("forecast: %d", forecast))

  ## icl: append data
  stan_data$N = c(stan_data$N,N)
  stan_data$y = c(stan_data$y,y[1]) # icl: just the index case!

  stan_data$x1=poly(1:N2,2)[,1]
  stan_data$x2=poly(1:N2,2)[,2]
  stan_data$SI=serial.interval$fit[1:N2]

  stan_data$covariate1 = cbind(stan_data$covariate1,covariates2[,1])
  stan_data$f = cbind(stan_data$f,f)
  stan_data$deaths = cbind(stan_data$deaths,deaths)
  stan_data$cases = cbind(stan_data$cases,cases)
  
  stan_data$N2=N2
  stan_data$x=1:N2
  if(length(stan_data$N) == 1) {
    stan_data$N = as.array(stan_data$N)
  }
}

# stan_data$covariate7 = 0 # icl: models should only take 6 covariates # -> ?

stan_data$y = t(stan_data$y)
options(mc.cores = parallel::detectCores())
rstan_options(auto_write = TRUE)
m = stan_model(paste0('../stan/',StanModel,'.stan'))

## here -> fix this whole section - parameterize, make it better - so ugly right now

# for now, doing it this way
fit = sampling(m,data=stan_data,iter=nStanIterations,warmup=nStanIterations/2,chains=8,thin=4,control = list(adapt_delta = 0.90, max_treedepth = 10))

# td: handle HMC convergence; see paper; consult with Phil.
# fit = sampling(m,data=stan_data,iter=4000,warmup=2000,chains=8,thin=4,control = list(adapt_delta = 0.90, max_treedepth = 10))

# big sim
# 25 counties -> 2.75 hrs
# 5 counties -> 20min
# fit = sampling(m,data=stan_data,iter=8000,warmup=4000,chains=8,thin=4,control = list(adapt_delta = 0.90, max_treedepth = 10))

# bigger sim
# 5 counties -> 66min -> pretty good
# 9 counties -> 106min
# fit = sampling(m,data=stan_data,iter=24000,warmup=12000,chains=8,thin=4,control = list(adapt_delta = 0.90, max_treedepth = 10))

# here -> just for testing that the code works
# fit = sampling(m,data=stan_data,iter=10,warmup=5,chains=2,thin=1,control = list(adapt_delta = 0.90, max_treedepth = 10))

# here -> upping the reps
# fit = sampling(m,data=stan_data, thin=1, control = list(adapt_delta = 0.90, max_treedepth = 10))

#### simulation is finished 

out = rstan::extract(fit)
prediction = out$prediction
estimated.deaths = out$E_deaths
estimated.deaths.cf = out$E_deaths0

JOBID = Sys.getenv("PBS_JOBID")
if(JOBID == "")
  JOBID = as.character(abs(round(rnorm(1) * 1000000)))
print(sprintf("Jobid = %s",JOBID))
save.image(paste0('../modelOutput/results/',StanModel,'-',JOBID,'.Rdata'))
save(fit,prediction,dates,reported_cases,deaths_by_country,countries,estimated.deaths,estimated.deaths.cf,out,covariates,file=paste0('../modelOutput/results/',StanModel,'-',JOBID,'-stanfit.Rdata'))

#### saving of simulation results is finished

#### now -> visualize model results -> ####

# icl: to visualize results

# library(bayesplot)
filename <- paste0(StanModel, '-', JOBID)

# plot_labels <- c("Lockdown")

# log(alpha)
# alpha = (as.matrix(out$alpha))
# colnames(alpha) = plot_labels
# g = (mcmc_intervals(alpha, prob = .9))
# ggsave(sprintf("../modelOutput/results/%s-covars-alpha-log.pdf",filename),g,width=4,height=6)

# alpha
# g = (mcmc_intervals(alpha, prob = .9,transformations = function(x) exp(-x)))
# ggsave(sprintf("../modelOutput/results/%s-covars-alpha.pdf",filename),g,width=4,height=6)

# R0
# mu = (as.matrix(out$mu))
# colnames(mu) = countries
# g = (mcmc_intervals(mu,prob = .9))
# ggsave(sprintf("../modelOutput/results/%s-covars-mu.pdf",filename),g,width=4,height=6)

# Rt -> better viz code in plot-static.r
# dimensions <- dim(out$Rt)
# Rt = (as.matrix(out$Rt[,dimensions[2],]))
# colnames(Rt) = countries
# g = (mcmc_intervals(Rt,prob = .9))
# ggsave(sprintf("../modelOutput/results/%s-covars-final-rt.pdf",filename),g,width=4,height=6)


# fixme: don't force other scripts to load R data -> unnecessary overhead
# import viz routine, call those functions here -> way, way better
# still save the R data and fit though, for backup, etc.


system(paste0("Rscript plot-trend.r ", filename,'.Rdata')) 
system(paste0("Rscript plot-forecast.r ", filename,'.Rdata')) ## icl: to run this code you will need to adjust manual values of forecast required

# suppressing for now
# system(paste0("Rscript plot-explore.r ", filename,'.Rdata'))
