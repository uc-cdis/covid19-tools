library(ggplot2)
library(tidyr)
library(dplyr)
library(rstan)
library(data.table)
library(lubridate)
library(gdata)
library(EnvStats)
library(matrixStats)
library(scales)
library(gridExtra)
library(ggpubr)
library(bayesplot)
library(cowplot)

source("utils/geom-stepribbon.r")
#---------------------------------------------------------------------------
make_forecast_plot <- function(){
  
  args <- commandArgs(trailingOnly = TRUE)
  filename <- args[1]
  
  load(paste0("../modelOutput/results/", filename))

  codeToName <- unique(data.frame("code" = d$countryterritoryCode, "name" = d$countriesAndTerritories))
  
  for(i in 1:length(countries)){
    N <- length(dates[[i]])

    # N2 should already be loaded from the loaded .rdata file
    # N2 <- N + 7 
    country <- countries[[i]]
    countryName <- as.character(codeToName$name[codeToName$code == country]) # this is the name -> "Cook"
    
    predicted_cases <- colMeans(prediction[,1:N,i])
    predicted_cases_li <- colQuantiles(prediction[,1:N,i], probs=.025)
    predicted_cases_ui <- colQuantiles(prediction[,1:N,i], probs=.975)

    # new
    predicted_cases_forecast <- colMeans(prediction[,1:N2,i])[N:N2]
    predicted_cases_li_forecast <- colQuantiles(prediction[,1:N2,i], probs=.025)[N:N2]
    predicted_cases_ui_forecast <- colQuantiles(prediction[,1:N2,i], probs=.975)[N:N2]

    estimated_deaths <- colMeans(estimated.deaths[,1:N,i])
    estimated_deaths_li <- colQuantiles(estimated.deaths[,1:N,i], probs=.025)
    estimated_deaths_ui <- colQuantiles(estimated.deaths[,1:N,i], probs=.975)
    
    estimated_deaths_forecast <- colMeans(estimated.deaths[,1:N2,i])[N:N2]
    estimated_deaths_li_forecast <- colQuantiles(estimated.deaths[,1:N2,i], probs=.025)[N:N2]
    estimated_deaths_ui_forecast <- colQuantiles(estimated.deaths[,1:N2,i], probs=.975)[N:N2]
    
    rt <- colMeans(out$Rt[,1:N,i])
    rt_li <- colQuantiles(out$Rt[,1:N,i],probs=.025)
    rt_ui <- colQuantiles(out$Rt[,1:N,i],probs=.975)
    
    data_country <- data.frame("time" = as_date(as.character(dates[[i]])),
                               "country" = rep(country, length(dates[[i]])),
                               #"country_population" = rep(country_population, length(dates[[i]])),
                               "reported_cases" = reported_cases[[i]], 
                               "reported_cases_c" = cumsum(reported_cases[[i]]), 
                               "predicted_cases_c" = cumsum(predicted_cases),
                               "predicted_min_c" = cumsum(predicted_cases_li),
                               "predicted_max_c" = cumsum(predicted_cases_ui),
                               "predicted_cases" = predicted_cases,
                               "predicted_min" = predicted_cases_li,
                               "predicted_max" = predicted_cases_ui,
                               "deaths" = deaths_by_country[[i]],
                               "deaths_c" = cumsum(deaths_by_country[[i]]),
                               "estimated_deaths_c" =  cumsum(estimated_deaths),
                               "death_min_c" = cumsum(estimated_deaths_li),
                               "death_max_c"= cumsum(estimated_deaths_ui),
                               "estimated_deaths" = estimated_deaths,
                               "death_min" = estimated_deaths_li,
                               "death_max"= estimated_deaths_ui,
                               "rt" = rt,
                               "rt_min" = rt_li,
                               "rt_max" = rt_ui)
    
    times <- as_date(as.character(dates[[i]]))
    times_forecast <- times[length(times)] + 0:(N2 - N) # the number of days to forecast
    data_country_forecast <- data.frame("time" = times_forecast,
                                        "country" = rep(country, N2 - N + 1), # p sure this works
                                        "estimated_deaths_forecast" = estimated_deaths_forecast,
                                        "death_min_forecast" = estimated_deaths_li_forecast,
                                        "death_max_forecast" = estimated_deaths_ui_forecast,
                                        # new
                                        "estimated_cases_forecast" = predicted_cases_forecast,
                                        "cases_min_forecast" = predicted_cases_li_forecast,
                                        "cases_max_forecast" = predicted_cases_ui_forecast
                                        )
    
    make_two_plots(data_country = data_country, 
                     data_country_forecast = data_country_forecast,
                     filename = filename,
                     country = countryName,
                     code = country)
    
  }
}

make_two_plots <- function(data_country, data_country_forecast, filename, country, code){

  countyDir <- file.path("../modelOutput/figures", code)
  dir.create(countyDir, showWarnings = FALSE)
  
  data_deaths <- data_country %>%
    select(time, deaths, estimated_deaths) %>%
    gather("key" = key, "value" = value, -time)
  
  data_deaths_forecast <- data_country_forecast %>%
    select(time, estimated_deaths_forecast) %>%
    gather("key" = key, "value" = value, -time)
  
  # Force less than 1 case to zero
  data_deaths$value[data_deaths$value < 1] <- NA
  data_deaths_forecast$value[data_deaths_forecast$value < 1] <- NA
  data_deaths_all <- rbind(data_deaths, data_deaths_forecast)
  
  p <- ggplot(data_country) +
    ggtitle(paste0(country, " County Daily Deaths")) + 
    geom_bar(data = data_country, aes(x = time, y = deaths), 
             fill = "coral4", stat='identity', alpha=0.5) + 
    geom_line(data = data_country, aes(x = time, y = estimated_deaths), 
              col = "deepskyblue4") + 
    # geom_line(data = data_country_forecast, 
    #           aes(x = time, y = estimated_deaths_forecast), 
    #           col = "black", alpha = 0.5) + 
    geom_ribbon(data = data_country, aes(x = time, 
                                         ymin = death_min, 
                                         ymax = death_max),
                fill="deepskyblue4", alpha=0.3) +
    # geom_ribbon(data = data_country_forecast, 
    #             aes(x = time, 
    #                 ymin = death_min_forecast, 
    #                 ymax = death_max_forecast),
    #             fill = "black", alpha=0.35) +
    # geom_vline(xintercept = data_deaths$time[length(data_deaths$time)], 
    #            col = "black", linetype = "dashed", alpha = 0.5) + 
    #scale_fill_manual(name = "", 
    #                 labels = c("Confirmed deaths", "Predicted deaths"),
    #                 values = c("coral4", "deepskyblue4")) + 
    xlab("Date") +
    ylab("Deaths\n") + 
    scale_x_date(date_breaks = "weeks", labels = date_format("%e %b")) + 
    scale_y_continuous(trans='log10', labels=comma) + 
    coord_cartesian(ylim = c(1, 100000), expand = FALSE) + 
    theme_pubr() + 
    theme(axis.text.x = element_text(angle = 45, hjust = 1),
      plot.title = element_text(hjust = 0.5)) + 
    guides(fill=guide_legend(ncol=1, reverse = TRUE)) # + 
    # annotate(geom="text", x=data_country$time[length(data_country$time)]+8, 
    #          y=10000, label="Forecast",
    #          color="black")
  
  # ggsave(file= paste0("../modelOutput/figures/", country, "_deaths_forecast_", filename, ".pdf"), p, width = 10)
  save_plot(filename = file.path(countyDir, "deathsForecast.png"), p)

  #### plot cases forecast ####

  data_cases <- data_country %>%
    select(time, reported_cases, predicted_cases) %>%
    gather("key" = key, "value" = value, -time)
  
  data_cases_forecast <- data_country_forecast %>%
    select(time, estimated_cases_forecast) %>%
    gather("key" = key, "value" = value, -time)
  
  # Force less than 1 case to zero (?)
  data_cases$value[data_cases$value < 1] <- NA
  data_cases_forecast$value[data_cases_forecast$value < 1] <- NA
  data_cases_all <- rbind(data_cases, data_cases_forecast)
  
  p <- ggplot(data_country) +
    ggtitle(paste0(country, " County Daily Cases")) + 
    geom_bar(data = data_country, aes(x = time, y = reported_cases), 
             fill = "coral4", stat='identity', alpha=0.5) + 
    geom_line(data = data_country, aes(x = time, y = predicted_cases), 
              col = "deepskyblue4") + 
    # geom_line(data = data_country_forecast, 
    #           aes(x = time, y = estimated_cases_forecast), 
    #           col = "black", alpha = 0.5) + 
    geom_ribbon(data = data_country, aes(x = time, 
                                         ymin = predicted_min, 
                                         ymax = predicted_max),
                fill="deepskyblue4", alpha=0.3) +
    # geom_ribbon(data = data_country_forecast, 
    #             aes(x = time, 
    #                 ymin = cases_min_forecast, 
    #                 ymax = cases_max_forecast),
    #             fill = "black", alpha=0.35) +
    # geom_vline(xintercept = data_cases$time[length(data_cases$time)], 
    #            col = "black", linetype = "dashed", alpha = 0.5) + 
    xlab("Date") +
    ylab("Cases\n") + 
    scale_x_date(date_breaks = "weeks", labels = date_format("%e %b")) + 
    scale_y_continuous(trans='log10', labels=comma) + 
    # may need to change ylim if not big enough 
    coord_cartesian(ylim = c(1, 100000), expand = FALSE) + 
    theme_pubr() + 
    theme(axis.text.x = element_text(angle = 45, hjust = 1),
      plot.title = element_text(hjust = 0.5)) + 
    guides(fill=guide_legend(ncol=1, reverse = TRUE)) # + 
    # might need to touch this - what's with the hardcoded 8?
    # annotate(geom="text", x=data_country$time[length(data_country$time)]+8, 
    #          y=10000, label="Forecast",
    #          color="black")
  
  save_plot(filename = file.path(countyDir, "casesForecast.png"), p)
}
#-----------------------------------------------------------------------------------------------
make_forecast_plot()

