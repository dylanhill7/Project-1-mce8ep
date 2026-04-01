# DS 4320 Project 1: Slugger Signal

This repository contains the materials for a data science project analyzing Major League Baseball player performance using the relational model. It includes the dataset used in the analysis, pipeline files that perform data preparation and statistical analysis, and a press release summarizing the project’s findings for a general audience. The repository also includes a README that provides an overview of the project and organizes the documentation into several sections, including a problem definition, domain exposition, data creation process, and metadata describing the dataset structure. Together, these components document the full workflow from data construction to analysis and interpretation.

Name: Dylan Hill

Net ID: mce8ep

DOI: <img width="191" height="20" alt="image" src="https://github.com/user-attachments/assets/4c328318-315e-4071-9aa5-0021692baab7" />


Link to Press Release: https://github.com/dylanhill7/Project-1-mce8ep/blob/main/Press-Release.md

Link to Data Folder: https://myuva-my.sharepoint.com/:f:/g/personal/mce8ep_virginia_edu/IgA33R9c9CWWSJC_mhF66DM_AZTih039TMHYSgmy73315zU?e=TBlCie

Link to Pipeline Files: https://github.com/dylanhill7/Project-1-mce8ep/tree/main/Pipeline-Files

Name of License and link to file in top level of the repository

## Problem Definition
Initial general problem: Projecting athletic performance

Refined specific problem statement: Which National League hitters are primed for breakout campaigns (age 20-25, drastic increase in mainstream hitting metrics, main focus on average/on base percentage/slugging percentage) in the upcoming 2026 MLB season, and which ones are due for some regression (age 30-35, drastic decline in same metrics)?

Rationale for refinement: The domain we started with for the general problem of 'projecting athletic performance' was not nearly specific enough. Though we were already in the category of sports, we needed to narrow it down to a specific sport and a league and I did that and took it one step further by choosing baseball, the MLB, and specifically the National League in the MLB (one of two leagues/conferences that 15 of 30 MLB teams play in). From there I took our lens of athletes in general to hitters in the MLB, and the objective of looking at these batters is to find the ones that we suspect will either rise or fall the most in terms of their year-over-year performance. I labeled these changes as 'breakout' or 'regression' seasons, and I thought it was important to clearly define what I meant by those two terms so that we are able to clearly identify the players who meet that criteria.

Motivation for the project: Sports analytics has been a big focus of my extracurricular involvement and career exploration during my time at UVA, and that all stems from an interest that I took in sabermetrics early on in highschool. After reading and watching 'Moneyball' and doing a summer program with Wharton focused on advanced analytics in sports I was hooked on predicting outcomes and performance for games and players. I believe the task of predicting player performance is a slightly more worthy task as individual game outcomes can be noisy and player performance, specifically MLB player performance (happening over the course of a 162 game season) is a more reliable indicator of how strong of a model you actually have. Hence why I chose to tackle a problem in sports analytics, specifically for the MLB. I wanted to explore hitters in particular because I believe the advanced analytics scene for offense is more advanced at this point than it is for other elements of baseball like pitching and fielding. Lastly I chose to explore the National League in the MLB because my favorite team is the Mets but I felt that the pool of data would be far too small if we looked just at the Mets roster, and I thought it would still be slightly too small if we looked specifically at the division the Mets play in (the NL East). So instead I decided to expand my scope to the entire conference/league the Mets play in, which is the National League.

Headline of press release: Rising Rakers and Aging Sluggers: Data-Driven Analysis of Top Breakout and Regression Candidates for the 2026 MLB Campaign (NL Edition)

Link to press release: 

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
