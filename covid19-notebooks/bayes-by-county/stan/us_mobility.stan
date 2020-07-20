data {
  int <lower=1> M; // number of countries
  int <lower=1> N0; // number of days for which to impute infections
  int<lower=1> N[M]; // days of observed data for country m. each entry must be <= N2
  int<lower=1> N2; // days of observed data + # of days to forecast
  int deaths[N2, M]; // reported deaths -- the rows with i > N contain -1 and should be ignored
  matrix[N2, M] f; // h * s
  int EpidemicStart[M];
  real SI[N2]; // fixed pre-calculated SI using emprical data from Neil
  // new data for mobility //
  int <lower=1> P_partial_county; // number of covariates for partial pooling (state-level effects)
  matrix[N2, P_partial_county] X_partial_county[M];
  int W; // number of weeks for weekly effects
  int week_index[M,N2];
}

parameters {
  real<lower=0> mu[M]; // intercept for Rt
  vector[P_partial_county] alpha_county[M];
  real<lower=0> kappa;
  real<lower=0> y[M];
  real<lower=0> phi;
  real<lower=0> tau;
  // new parameters
  real<lower=0> gamma_county;
  matrix[W+1,M] weekly_effect;
  real<lower=0, upper=1> weekly_rho;
  real<lower=0, upper=1> weekly_rho1;
  real<lower=0> weekly_sd;

}

transformed parameters {
  real convolution;
  matrix[N2, M] prediction = rep_matrix(0,N2,M);
  matrix[N2, M] E_deaths  = rep_matrix(0,N2,M);
  matrix[N2, M] Rt = rep_matrix(0,N2,M);
  for (m in 1:M){
    prediction[1:N0,m] = rep_vector(y[m],N0); // learn the number of cases in the first N0 days
    Rt[,m] = mu[m] * 2 * inv_logit(-X_partial_county[m] * alpha_county[m] - weekly_effect[week_index[m],m]);
    for (i in (N0+1):N2) {
      convolution=0;
      for(j in 1:(i-1)) {
        convolution += prediction[j, m]*SI[i-j]; // Correctd 22nd March
      }
      prediction[i, m] = Rt[i,m] * convolution;
    }
    
    E_deaths[1, m]= 1e-9;
    for (i in 2:N2){
      E_deaths[i,m]= 0;
      for(j in 1:(i-1)){
        E_deaths[i,m] += prediction[j,m]*f[i-j,m];
      }
    }
  }
}

model {
  tau ~ exponential(0.03);
  gamma_county ~ normal(0,.5);
  weekly_sd ~ normal(0,0.2);
  weekly_rho ~ normal(0.8, 0.05);
  weekly_rho1 ~ normal(0.1, 0.05);
  kappa ~ normal(0,0.5);
  mu ~ normal(3.28, kappa); // citation: https://academic.oup.com/jtm/article/27/2/taaa021/5735319
  phi ~ normal(0,5);
  for (m in 1:M) {
      alpha_county[m] ~ normal(0,gamma_county);
      y[m] ~ exponential(1/tau);
      weekly_effect[3:(W+1), m] ~ normal(weekly_effect[2:W,m]* weekly_rho + weekly_effect[1:(W-1),m]* weekly_rho1, 
                                            weekly_sd *sqrt(1-pow(weekly_rho,2)-pow(weekly_rho1,2) - 2 * pow(weekly_rho,2) * weekly_rho1/(1-weekly_rho1)));
      for(i in EpidemicStart[m]:N[m]){
        deaths[i,m] ~ neg_binomial_2(E_deaths[i,m],phi); 
      }
  }
  weekly_effect[2, ] ~ normal(0,weekly_sd *sqrt(1-pow(weekly_rho,2)-pow(weekly_rho1,2) - 2 * pow(weekly_rho,2) * weekly_rho1/(1-weekly_rho1)));
  weekly_effect[1, ] ~ normal(0, 0.01);
}

