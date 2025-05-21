# === Package Installer ===
packages <- c("ggplot2", "dplyr", "ggpubr", "gridExtra", "ggplotify", "readr", "tidyr", "purrr", "stringr", "extrafont", "scales")
# Set CRAN mirror
options(repos = c(CRAN = "https://cloud.r-project.org"))

installed <- rownames(installed.packages())
for (pkg in packages) {
  if (!(pkg %in% installed)) {
    install.packages(pkg, dependencies = TRUE)
  }
}

library(ggplot2)
library(dplyr)
library(ggpubr)
library(gridExtra)
library(ggplotify)
library(readr)
library(tidyr)
library(purrr)
library(stringr)
library(extrafont)
library(scales)

# font_import()

# Example centralized and decentralized data
# Replace this with your actual data frames
centralized_1 <- read.csv("results/results_runs_1.csv")
centralized_3 <- read.csv("results/results_runs_3.csv")
decentralized <- read.csv("results/results_daily_runs_city.csv")
decentralized_upsert <- read.csv("results/results_daily_runs_city_upsert_min_agg_100_all.csv")
decentralized_min_agg <- read.csv("results/results_daily_runs_city_min_agg_2_all.csv")
decentralized_min_agg_90 <- read.csv("results/results_daily_runs_city_min_agg_100_90.csv")

# Add source labels
centralized_1$system <- "Centralized, 1 run/day"
centralized_3$system <- "Centralized, 3 runs/day"
decentralized$system <- "Decentralized"
decentralized_min_agg$system <- "Decentralized, min. agg. 2, 100% selectivity"
decentralized_min_agg_90$system <- "Decentralized, min. agg. 100, 90% selectivity"
decentralized_upsert$system <- "Decentralized, UPSERT, min. agg. 100, 100% selectivity"

# Combine data
all_data_1 <- bind_rows(centralized_1, centralized_3, decentralized)
all_data_2 <- bind_rows(centralized_1, decentralized_min_agg, decentralized_min_agg_90, decentralized_upsert)

# === AGGREGATE SELECTED QUERIES ===
# Change these to include/exclude more queries
queries_to_exclude <- c("query1", "query2")

agg_data_1 <- all_data_1 %>%
  filter(!query_hash %in% queries_to_exclude) %>%
  group_by(run, system) %>%
  summarise(total_time = sum(time_ms), .groups = "drop")

agg_data_2 <- all_data_2 %>%
  filter(!query_hash %in% queries_to_exclude) %>%
  group_by(run, system) %>%
  summarise(total_time = sum(time_ms), .groups = "drop")

# === PLOT ===
plot_1 <- ggplot(agg_data_1, aes(x = run, y = total_time, color = system)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 2) +
  labs(x = "Run", y = "Total Time (ms)", color = "System") +
  scale_y_continuous(labels = comma, breaks = seq(0, 8000, by = 1000)) +
  theme_minimal(base_size = 14, base_family = "Linux Libertine") +
  #scale_color_manual(values = c("Centralized" = "#0072B2", "Decentralized" = "#D55E00")) +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    legend.position = "top"
  )

# Save the plot
png("plots/centralized_vs_decentralized_plot.png", width = 2000, height = 1200, res = 200)
print(plot_1)
dev.off()

plot_2 <- ggplot(agg_data_2, aes(x = run, y = total_time, color = system)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 2) +
  labs(x = "Run", y = "Total Time (ms)", color = "System") +
  scale_y_continuous(labels = comma, breaks = seq(0, 8000, by = 1000)) +
  theme_minimal(base_size = 14, base_family = "Linux Libertine") +
  #scale_color_manual(values = c("Centralized" = "#0072B2", "Decentralized" = "#D55E00")) +
  theme(plot.title = element_text(face = "bold", hjust = 0.5), legend.position = "top") +
  guides(color = guide_legend(ncol = 2))

# Save the plot
png("plots/minimum_aggregation_plot.png", width = 2000, height = 1200, res = 200)
print(plot_2)
dev.off()