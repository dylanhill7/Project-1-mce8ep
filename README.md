# DS 4320 Project 1: Slugger Signal

This repository contains the materials for a data science project analyzing Major League Baseball player performance using the relational model. It includes the dataset used in the analysis, pipeline files that perform data preparation and statistical analysis, and a press release summarizing the project’s findings for a general audience. The repository also includes a README that provides an overview of the project and organizes the documentation into several sections, including a problem definition, domain exposition, data creation process, and metadata describing the dataset structure. Together, these components document the full workflow from data construction to analysis and interpretation.

Name: Dylan Hill

Net ID: mce8ep

DOI: <img width="191" height="20" alt="image" src="https://github.com/user-attachments/assets/4c328318-315e-4071-9aa5-0021692baab7" />


Link to Press Release: https://github.com/dylanhill7/Project-1-mce8ep/blob/main/Press-Release.md

Link to Data Folder: https://myuva-my.sharepoint.com/:f:/g/personal/mce8ep_virginia_edu/IgA33R9c9CWWSJC_mhF66DM_AZTih039TMHYSgmy73315zU?e=TBlCie

Link to Pipeline Files: https://github.com/dylanhill7/Project-1-mce8ep/tree/main/Pipeline-Files

Name of License: mce8ep-LICENSE

Link to License: https://github.com/dylanhill7/Project-1-mce8ep/blob/main/mce8ep-LICENSE

## Problem Definition
Initial general problem: Projecting athletic performance

Refined specific problem statement: Which National League hitters are primed for breakout campaigns (age 20-25, drastic increase in mainstream hitting metrics, main focus on average/on base percentage/slugging percentage) in the upcoming 2026 MLB season, and which ones are due for some regression (age 30-35, drastic decline in same metrics)?

Rationale for refinement: The domain we started with for the general problem of 'projecting athletic performance' was not nearly specific enough. Though we were already in the category of sports, we needed to narrow it down to a specific sport and a league and I did that and took it one step further by choosing baseball, the MLB, and specifically the National League in the MLB (one of two leagues/conferences that 15 of 30 MLB teams play in). From there I took our lens of athletes in general to hitters in the MLB, and the objective of looking at these batters is to find the ones that we suspect will either rise or fall the most in terms of their year-over-year performance. I labeled these changes as 'breakout' or 'regression' seasons, and I thought it was important to clearly define what I meant by those two terms so that we are able to clearly identify the players who meet that criteria.

Motivation for the project: Sports analytics has been a big focus of my extracurricular involvement and career exploration during my time at UVA, and that all stems from an interest that I took in sabermetrics early on in highschool. After reading and watching 'Moneyball' and doing a summer program with Wharton focused on advanced analytics in sports I was hooked on predicting outcomes and performance for games and players. I believe the task of predicting player performance is a slightly more worthy task as individual game outcomes can be noisy and player performance, specifically MLB player performance (happening over the course of a 162 game season) is a more reliable indicator of how strong of a model you actually have. Hence why I chose to tackle a problem in sports analytics, specifically for the MLB. I wanted to explore hitters in particular because I believe the advanced analytics scene for offense is more advanced at this point than it is for other elements of baseball like pitching and fielding. Lastly I chose to explore the National League in the MLB because my favorite team is the Mets but I felt that the pool of data would be far too small if we looked just at the Mets roster, and I thought it would still be slightly too small if we looked specifically at the division the Mets play in (the NL East). So instead I decided to expand my scope to the entire conference/league the Mets play in, which is the National League.

Headline of press release: Rising Rakers and Aging Sluggers: Data-Driven Analysis of Top Breakout and Regression Candidates for the 2026 MLB Campaign (NL Edition)

Link to press release: https://github.com/dylanhill7/Project-1-mce8ep/blob/main/Press-Release.md

## Domain Exposition

Terminology:

| Term | Type | Definition |
|-----|-----|-----|
| wRC+ (Weighted Runs Created Plus) | Jargon | An advanced offensive metric that measures a player's total run creation and adjusts for league and ballpark context, where **100 represents league-average performance** and each point above or below reflects a percentage difference from average. |
| wOBA (Weighted On-Base Average) | Jargon | An advanced offensive statistic that assigns different weights to offensive events (walks, singles, doubles, etc.) to better measure a hitter’s overall offensive value per plate appearance. |
| BABIP (Batting Average on Balls in Play) | Jargon | A statistic measuring how often a ball put into play becomes a hit, often used to evaluate whether a player’s performance may be influenced by luck or likely to regress toward their long-term average. |
| Exit Velocity | Jargon | The speed of the baseball as it leaves the bat, used to measure the quality and strength of a hitter’s contact. |
| Launch Angle | Jargon | The vertical angle at which the ball leaves the bat, commonly used to analyze hitting mechanics and the likelihood of producing extra-base hits or home runs. |
| WAR (Wins Above Replacement) | Jargon | An advanced metric estimating how many additional wins a player contributes to their team compared with a replacement-level player. |
| Barrel Rate | Jargon | The percentage of batted balls hit with an optimal combination of exit velocity and launch angle that typically results in strong offensive outcomes such as extra-base hits. |
| K% (Strikeout Rate) | Jargon | The percentage of a player’s plate appearances that end in a strikeout, used to evaluate a hitter’s ability to make contact and avoid strikeouts. |
| Hits | KPI | The number of times a batter successfully reaches base by hitting the ball into play without the benefit of an error or fielder’s choice. |
| Runs Batted In (RBIs) | KPI | The number of runs scored as a direct result of a player’s plate appearance, excluding runs scored due to errors or double plays. |
| Home Runs | KPI | The number of times a batter hits the ball out of the field of play in fair territory, allowing them to circle all bases and score. |
| Batting Average (AVG) | KPI | The ratio of hits to at-bats, representing how frequently a hitter records a hit. |
| On-Base Percentage (OBP) | KPI | A statistic measuring how often a player reaches base through hits, walks, or hit-by-pitch divided by total plate appearances. |
| Slugging Percentage (SLG) | KPI | A statistic measuring a hitter’s power by calculating total bases divided by at-bats. |
| OPS (On-Base Plus Slugging) | KPI | A combined offensive metric calculated as **OBP + SLG**, used to summarize a hitter’s overall ability to both reach base and hit for power. |

Domain Explanation: This project exists within the domain of sports analytics, specifically the field of baseball analytics often referred to as sabermetrics. Sabermetrics focuses on using statistical analysis and data-driven methods to evaluate player performance and predict future outcomes. In Major League Baseball, teams increasingly rely on advanced metrics and large datasets to guide roster decisions, player development strategies, and game planning. Offensive performance in particular has become highly quantifiable through both traditional statistics—such as batting average, home runs, and runs batted in—and more advanced metrics like wRC+, launch angle, exit velocity, and barrel rate. These statistics provide insight into both the results a player produces and the underlying quality of their contact at the plate. By analyzing these performance indicators over time, analysts can identify patterns that signal whether a hitter is likely to improve significantly (a breakout season) or regress from previous performance levels. This project sits at the intersection of predictive analytics and sports performance analysis, using historical player statistics to identify National League hitters who may experience major changes in offensive production during the 2026 MLB season.

Background reading: https://drive.google.com/drive/folders/1cxH_RrCYLtKwSGUqPwno0cDVrlP3R2OV?usp=drive_link

Table: 

| Title of Article | Brief Description | Link |
|---|---|---|
| Sabermetrics & Baseball Analytics: The Science of Winning | Overview article explaining how sabermetrics uses statistical analysis and advanced metrics to evaluate baseball player performance and inform team decision-making. It highlights how data-driven strategies help teams identify undervalued players and predict future performance. | https://onlinegrad.syracuse.edu/blog/sabermetrics-baseball-analytics-the-science-of-winning-accessible/ |
| Exploring Key Metrics and Methodology for Analyzing Offensive Performance | Article outlining a methodology for evaluating baseball hitters using advanced metrics such as wRC+, plate discipline measures, and batted-ball quality data to better understand offensive production. | https://adamsalorio.substack.com/p/exploring-key-metrics-and-methodology-2b7 |
| The Prediction of Batting Averages in Major League Baseball | Research paper analyzing how Statcast data (exit velocity, launch angle, and hit distance) can be used to predict future batting averages. The study combines Statcast-based predictions with PECOTA forecasts and finds that combining multiple prediction methods improves forecasting accuracy. | https://www.sfu.ca/~tswartz/papers/sarah.pdf |
| Aging Gracefully: Approaching Aging Curves and Advanced Stats, Part I | Article examining how player performance changes with age using advanced baseball statistics. It explores aging curves and shows that many offensive metrics improve rapidly early in a player’s career before peaking in the late 20s, providing insight into when players are likely to break out or begin declining. | https://thedynastyguru.com/2019/02/18/aging-gracefully-approaching-aging-curves-and-advanced-stats-part-i/ |
| wRC and wRC+ | FanGraphs library article explaining the advanced metric wRC+, which measures a player’s overall offensive production while adjusting for ballpark and league context. The statistic is scaled so that 100 represents league-average performance, with values above or below indicating better or worse offensive output. | https://library.fangraphs.com/offense/wrc/ |

## Data Creation

Raw data acquisition process: obtained from FanGraphs, a widely used public baseball analytics platform that provides detailed player performance statistics for Major League Baseball (MLB). FanGraphs aggregates and publishes both traditional and advanced sabermetric statistics derived from official MLB game data and Statcast tracking systems. Using the FanGraphs leaderboard export tool, datasets were downloaded for National League hitters within specific age ranges relevant to the research question. Two primary age cohorts were collected: hitters aged 20–25 (to analyze potential breakout candidates) and hitters aged 30–35 (to analyze potential regression candidates). The data exports included season-level statistics for each player across multiple years, covering the period 1985–2019 and 2021–2025, with the 2020 season intentionally excluded due to the shortened COVID-19 schedule which could distort year-over-year comparisons.

For each age cohort, the FanGraphs export tool was used to generate CSV files containing both mainstream batting metrics (such as games played, plate appearances, batting average, on-base percentage, and slugging percentage) and advanced sabermetric statistics (including walk rate, strikeout rate, isolated power, BABIP, wOBA, wRC+, baserunning value, offensive value, defensive value, and WAR). These CSV files were stored in the project’s Data/ directory and ingested into a DuckDB relational database using a Python loading script. The ingestion process standardized column formats and preserved the original season information from FanGraphs. This structured dataset then served as the foundation for subsequent transformation steps, where player identifiers, season identifiers, and relational tables were created to support the modeling pipeline used to identify potential breakout and regression candidates.

Code used to create the data: 

| Code Link | Description |
|---|---|
| https://colab.research.google.com/drive/1liIVvDibnFYMhE5swUUwLC2N83WD-lZD?usp=sharing | **load.py**: Loads raw Fangraphs CSV data for hitters aged 20–25 and 30–35 into a DuckDB database. The script reads multiple season files, appends them into two staging tables (`hitters_20_25` and `hitters_30_35`), performs basic validation checks, and logs summary statistics such as total rows, seasons covered, and number of unique players. |
| https://colab.research.google.com/drive/1T3PS8zXDsNYDfzCwhey1itX68JRhx90p?usp=sharing | **transform1.py**: Transforms the staging tables into a normalized relational database structure. The script generates player and season lookup tables, assigns unique `player_id` and `season_id` keys, and creates four statistical tables separating mainstream and advanced batting metrics for breakout (age 20–25) and regression (age 30–35) cohorts. It also calculates OPS and exports the resulting relational tables to CSV. |

Bias Identification: Bias could be introduced in the data collection process through several design choices made when assembling the dataset from FanGraphs. First, the dataset only includes players who reached a minimum threshold of 200 plate appearances, which helps ensure statistical reliability but also introduces selection bias by excluding players with limited playing time. This may disproportionately remove younger players, injured players, or those used in platoon roles, potentially skewing the sample toward more established or consistently playing hitters. Additionally, the project focuses exclusively on National League hitters within specific age ranges (20–25 and 30–35), which intentionally narrows the population but may introduce sampling bias, as the model does not learn from players outside those age groups or from American League players who may experience different developmental or performance trajectories. Finally, the exclusion of the 2020 season due to its shortened schedule removes a potentially informative year and could slightly affect long-term trends in the data. While these choices were made to improve data quality and comparability across seasons, they may still influence which types of players and performance patterns are represented in the final dataset.

Bias Mitigation: Bias in the dataset can be mitigated and quantified through several analytical steps. First, the impact of the 200 plate appearance threshold can be assessed by comparing summary statistics (e.g., average OPS, wRC+, and age distribution) between players just above the cutoff and those below it when possible. If large differences appear, this indicates selection bias in the sample. Second, uncertainty in model predictions can be quantified using the cross-validation process already built into the pipeline, where performance metrics such as accuracy are averaged across multiple folds. The variance across these folds provides an estimate of how sensitive the model is to different training samples. For example, if the model’s cross-validated accuracy is 0.76 with a standard deviation of 0.03 across folds, this indicates that the true performance of the model likely falls within roughly ±3 percentage points depending on the training sample. Additionally, prediction uncertainty can be communicated using the predicted probabilities from the random forest model, rather than relying solely on binary breakout/regression labels. Finally, the use of percentile-based thresholds (75th percentile for breakouts and 25th percentile for regression) helps standardize how extreme performance changes are defined, ensuring that the classification of breakout and regression seasons is based on the empirical distribution of historical OPS changes rather than arbitrary cutoffs. Together, these approaches help both identify and quantify potential bias and uncertainty, improving transparency in the modeling results.

Rationale for Critical Decisions: Several critical design decisions were made during the project that required judgment calls and could introduce or mitigate uncertainty. One important decision was setting the minimum plate appearance threshold at 200 PA when collecting data from FanGraphs. This threshold was chosen to ensure that player statistics were based on a meaningful sample size (roughly one plate appearance per team game across a season), reducing noise from extremely small samples. However, this decision also excludes players with limited playing time, which could introduce selection bias. Another key judgment involved defining breakout and regression thresholds using the empirical distribution of year-over-year OPS changes, specifically the 75th percentile for breakout seasons and the 25th percentile for regression seasons. This approach avoids arbitrary cutoffs and ensures that labels reflect relatively extreme changes in performance, though the exact percentile choice still influences how many observations are classified as breakouts or regressions.

Additional decisions were made to reduce uncertainty in the modeling pipeline. The 2020 MLB season was excluded because the shortened COVID-19 schedule produced unusually small sample sizes and atypical performance patterns that could distort year-to-year comparisons. In the preprocessing stage, the xwOBA feature was removed because a large proportion of observations contained missing values; retaining it would have required heavy imputation that could introduce additional noise. Remaining missing values in other features were handled using median imputation, which preserves the central tendency of the data while limiting the influence of outliers. Finally, model uncertainty was addressed by implementing an 80/20 train–test split combined with 5-fold cross-validation and hyperparameter tuning for the random forest classifier. This process helps ensure that model performance estimates are not overly dependent on a single sample split and provides a more reliable assessment of how well the model generalizes to new data.

## Metadata

Schema:


Data:

| Table Name | Brief Description | Link |
|---|---|---|
| players | Lookup table containing a unique `player_id` for every player in the dataset along with the player’s name. This table enables consistent referencing of players across all other relational tables. | https://drive.google.com/file/d/1sLU4UZYEiLPEEgOTleZYI70hn-fh0J7n/view?usp=sharing |
| seasons | Lookup table mapping each MLB season to a unique `season_id`. This table standardizes season references across the database and supports year-to-year comparisons. | https://drive.google.com/file/d/18YseNc34m7EQnmef13f8K09SdCn0LvEI/view?usp=sharing |
| mainstream_batting_stats_breakout | Contains traditional batting statistics for players aged 20–25 (breakout candidates) by player and season. Includes metrics such as games played, plate appearances, HR, R, RBI, SB, AVG, OBP, SLG, and the derived OPS statistic. | https://drive.google.com/file/d/10bWgU9fVL18BWsQUh257iWhDH4TO0_8Q/view?usp=sharing |
| advanced_batting_stats_breakout | Contains advanced sabermetric statistics for players aged 20–25 (breakout candidates) by player and season. Features include metrics such as walk rate, strikeout rate, ISO, BABIP, wOBA, wRC+, baserunning value, offensive value, defensive value, and WAR. | https://drive.google.com/file/d/1M7V1vq7eKC69RnbZuLQ0cBP_4sn41Bt1/view?usp=sharing |
| mainstream_batting_stats_regression | Contains traditional batting statistics for players aged 30–35 (regression candidates) by player and season, including games played, plate appearances, HR, R, RBI, SB, AVG, OBP, SLG, and OPS. | https://drive.google.com/file/d/1m3DP8nzClkBSHgZ_HZAQwz1oVdxaiEeU/view?usp=sharing |
| advanced_batting_stats_regression | Contains advanced sabermetric statistics for players aged 30–35 (regression candidates) by player and season, including BB%, K%, ISO, BABIP, wOBA, wRC+, BsR, Off, Def, and WAR. | https://drive.google.com/file/d/1QZhQdC_3bN9Jqd49-96ejJ254QtFN7dl/view?usp=sharing |

Data Dictionary (overview):

| Name | Data Type | Description | Example |
|---|---|---|---|
| player_id | Integer | Unique identifier assigned to each player in the dataset. Used to link player records across tables. | 145 |
| player_name | String | Full name of the MLB player. Stored in the players lookup table. | Freddie Freeman |
| season_id | Integer | Unique identifier for each MLB season used to standardize year references across tables. | 37 |
| year | Integer | Calendar year corresponding to the MLB season. | 2023 |
| G | Integer | Number of games played by the player during the season. | 155 |
| PA | Integer | Total plate appearances for the player during the season. | 674 |
| HR | Integer | Total home runs hit by the player during the season. | 32 |
| R | Integer | Total runs scored by the player during the season. | 104 |
| RBI | Integer | Total runs batted in by the player during the season. | 96 |
| SB | Integer | Total stolen bases by the player during the season. | 18 |
| AVG | Float | Batting average, calculated as hits divided by at-bats. | 0.298 |
| OBP | Float | On-base percentage, measuring how often a player reaches base via hit, walk, or hit-by-pitch. | 0.378 |
| SLG | Float | Slugging percentage, measuring total bases per at-bat and capturing power hitting. | 0.512 |
| OPS | Float | On-base plus slugging percentage (OBP + SLG), used as a combined measure of offensive performance. | 0.890 |
| BB% | Float | Walk rate, representing the percentage of plate appearances that result in a walk. | 11.2 |
| K% | Float | Strikeout rate, representing the percentage of plate appearances that end in a strikeout. | 19.6 |
| ISO | Float | Isolated power, measuring a hitter’s raw power by capturing extra-base hit ability (SLG − AVG). | 0.214 |
| BABIP | Float | Batting average on balls in play, measuring how often a ball in play results in a hit. | 0.312 |
| wOBA | Float | Weighted on-base average, an advanced metric that assigns run values to different offensive outcomes. | 0.368 |
| wRC+ | Integer | Weighted runs created plus, measuring offensive production relative to league average (100 = league average). | 142 |
| BsR | Float | Baserunning runs, estimating the number of runs a player contributes through baserunning. | 3.5 |
| Off | Float | Offensive runs above average contributed by the player. | 24.1 |
| Def | Float | Defensive runs above average contributed by the player. | 5.2 |
| WAR | Float | Wins above replacement, estimating the total number of wins a player contributes compared to a replacement-level player. | 5.8 |

Data Dictionary (quantification of uncertainty):

- G (Games Played) — quantify uncertainty using the proportion of games played relative to a full season (G / 162) to reflect how representative the season sample is.
- PA (Plate Appearances) — quantify uncertainty using standard error scaling with 1/ sq root of PA, since larger PA reduces sampling variability in rate statistics.
- HR (Home Runs) — quantify uncertainty using a binomial or Poisson confidence interval for HR rate (HR / PA).
- R (Runs Scored) — quantify uncertainty using variance in runs per plate appearance (R / PA) across seasons or through bootstrap resampling.
- RBI (Runs Batted In) — quantify uncertainty using standard deviation of RBI per PA across seasons, accounting for context effects.
- SB (Stolen Bases) — quantify uncertainty using a binomial confidence interval on stolen base attempts or SB rate per PA.
- AVG (Batting Average) — quantify uncertainty using the binomial standard error
- OBP (On-Base Percentage) — quantify uncertainty using a binomial confidence interval based on plate appearances.
- SLG (Slugging Percentage) — quantify uncertainty using the variance of total bases per at-bat or bootstrap confidence intervals.
- OPS (On-base + Slugging) — quantify uncertainty by propagating uncertainty from OBP and SLG using variance addition.
- BB% (Walk Rate) — quantify uncertainty using a binomial standard error based on walks and PA.
- K% (Strikeout Rate) — quantify uncertainty using a binomial standard error based on strikeouts and PA.
- ISO (Isolated Power) — quantify uncertainty using bootstrap confidence intervals of ISO across plate appearances.
- BABIP — quantify uncertainty using a binomial confidence interval on balls in play or comparing deviation from league mean BABIP.
- wOBA — quantify uncertainty using bootstrap confidence intervals on weighted offensive outcomes.
- wRC+ — quantify uncertainty using the variance in run contribution estimates relative to league average across seasons.
- BsR (Baserunning Runs) — quantify uncertainty using standard deviation of baserunning run estimates derived from event-level outcomes.
- Off (Offensive Runs Above Average) — quantify uncertainty through propagated variance from underlying offensive metrics used in its calculation.
- Def (Defensive Runs Above Average) — quantify uncertainty using confidence intervals from defensive metric models or year-to-year variance.
- WAR (Wins Above Replacement) — quantify uncertainty by propagating error from batting, baserunning, and defensive components, often estimated at roughly ±0.5–1 WAR per season.
