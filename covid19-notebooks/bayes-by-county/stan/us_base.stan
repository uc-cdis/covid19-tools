data {
  int <lower=1> M; // number of countries
  int <lower=1> N0; // number of days for which to impute infections
  int<lower=1> N[M]; // days of observed data for country m. each entry must be <= N2
  int<lower=1> N2; // days of observed data + # of days to forecast
  real<lower=0> x[N2]; // index of days (starting at 1)
  int cases[N2,M]; // reported cases
  int deaths[N2, M]; // reported deaths -- the rows with i > N contain -1 and should be ignored
  matrix[N2, M] f; // h * s
  matrix[N2, M] covariate1;
  int EpidemicStart[M];
  real SI[N2]; // fixed pre-calculated SI using emprical data from Neil
}

transformed data {
  real delta = 1e-5;
}

parameters {
  real<lower=0> mu[M]; // intercept for Rt
  real<lower=0> alpha[1]; // icl: the hier term // -> ncols corresponds to number of covariates (i.e., number of interventions)
  real<lower=0> kappa;
  real<lower=0> y[M];
  real<lower=0> phi;
  real<lower=0> tau;
}

transformed parameters {
    real convolution;
    matrix[N2, M] prediction = rep_matrix(0,N2,M);
    matrix[N2, M] E_deaths  = rep_matrix(0,N2,M);
    matrix[N2, M] Rt = rep_matrix(0,N2,M);
    for (m in 1:M){
      prediction[1:N0,m] = rep_vector(y[m],N0); // learn the number of cases in the first N0 days
        Rt[,m] = mu[m] * exp(covariate1[,m] * (-alpha[1])); // + GP[i]); // to_vector(x) * time_effect
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
   /* for(m in 1:M) {
     for(i in 1:N[m]) {
      LowerBound[i,m] = prediction[i,m] * 10 - cases[i,m];
     }
    }*/

}
model {
  tau ~ exponential(0.03);
  for (m in 1:M){
      y[m] ~ exponential(1.0/tau);
  }
  phi ~ normal(0,5);
  kappa ~ normal(0,0.5);
  mu ~ normal(2.4, kappa); // citation needed
  alpha ~ gamma(.5,1);
  for(m in 1:M){
    for(i in EpidemicStart[m]:N[m]){
       deaths[i,m] ~ neg_binomial_2(E_deaths[i,m],phi);
    }
   }
}

generated quantities {
    matrix[N2, M] lp0 = rep_matrix(1000,N2,M); // log-probability for LOO for the counterfactual model
    matrix[N2, M] lp1 = rep_matrix(1000,N2,M); // log-probability for LOO for the main model
    real convolution0;
    matrix[N2, M] prediction0 = rep_matrix(0,N2,M);
    matrix[N2, M] E_deaths0  = rep_matrix(0,N2,M);
    for (m in 1:M){
      prediction0[1:N0,m] = rep_vector(y[m],N0); // learn the number of cases in the first N0 days
      for (i in (N0+1):N2) {
        convolution0=0;
        for(j in 1:(i-1)) {
          convolution0 += prediction0[j, m]*SI[i-j]; // Correctd 22nd March
        }
        prediction0[i, m] = mu[m] * convolution0;
      }

      E_deaths0[1, m]= 1e-9;
      for (i in 2:N2){
        E_deaths0[i,m]= 0;
        for(j in 1:(i-1)){
          E_deaths0[i,m] += prediction0[j,m]*f[i-j,m];
        }
      }
      for(i in 1:N[m]){
        lp0[i,m] = neg_binomial_2_lpmf(deaths[i,m] | E_deaths[i,m],phi);
        lp1[i,m] = neg_binomial_2_lpmf(deaths[i,m] | E_deaths0[i,m],phi);
      }
    }

}
