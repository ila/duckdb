# === Package Installer ===
packages <- c("ggplot2", "dplyr", "ggpubr", "gridExtra", "ggplotify", "readr", "tidyr", "purrr", "stringr", "extrafont")
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

fonts_import()

# Example centralized and decentralized data
# Replace this with your actual data frames
centralized <- read.csv("results_runs.csv")
decentralized <- read.csv("results_daily_runs_city.csv")

# Add source labels
centralized$system <- "Centralized"
decentralized$system <- "Decentralized"

# Combine data
all_data <- bind_rows(centralized, decentralized)

# === AGGREGATE SELECTED QUERIES ===
# Change these later to include/exclude more queries
queries_to_exclude <- c("query1", "query2")

agg_data <- all_data %>%
  filter(!query_hash %in% queries_to_exclude) %>%
  group_by(run, system) %>%
  summarise(total_time = sum(time_ms), .groups = "drop")

# === PLOT ===
plot_1 <- ggplot(agg_data, aes(x = run, y = total_time, color = system)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 2) +
  labs(
    x = "Run",
    y = "Total Time (ms)",
    color = "System"
  ) +
  theme_minimal(base_size = 14, base_family = "Libertine") +
  scale_color_manual(values = c("Centralized" = "#0072B2", "Decentralized" = "#D55E00")) +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    legend.position = "top"
  )

# Save the plot
png("plots/centralized_vs_decentralized_plot.png", width = 800, height = 600, res = 100)
print(plot_1)
dev.off()