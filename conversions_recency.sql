#!/bin/bash
# https://cs.corp.google.com/piper///depot/google3/experimental/users/zhangdm/rmkt/latency/ts-diff.borg?l=75
# using UserListEventLog maintained by remarketing quality team. 
# This log is processed log based on AdEventsLog.
# The field I was looking into is iba_calibration_data, which is also available
# in qem.impression_set.content_impression.iba_calibration_data.
# For example, here is a query to get sample latest timestamp when the user
# visited the advertiser's site. To figure out the user recency, we can use
# this timestamp and compare it with the timestamp of the ad query.

# A time = visit a website and added to user_list
# B time = shown remarketing ad on a pub site
# unviewed_ is usually for external exchanges

# https://cs.corp.google.com/piper///depot/google3/logs/lib/contentads/dremel/gmob.macro?rcl=144845365&l=10
# MOBILE_BROWSER_LOWEND = 0;
# MOBILE_BROWSER_MIDRANGE = 1;
# MOBILE_BROWSER_HIGHEND = 2;
# DESKTOP_BROWSER = 3;
# TABLET_BROWSER = 4;
# TV_BROWSER = 5;
# GAME_CONSOLE_BROWSER = 6;

MATERIALIZE '/cns/ig-d/home/aredakov/gdn_recency/delta_conversions_distribution/data@500' AS
SELECT
  impression_set.customer_id customer_id,
  impressions.clicks.conversions.conversion.conversion_id.process_id
    conversion_id,
  # If this field is not a empty string, then dynamic RMKT. qchong@
  IF(impression_set.content_impression.gpa_ad_info.use_case_name != '',
    'dynamic','static') rmkt_type,
  CASE
    WHEN (query.mobile_browser_class IN (0,1,2,4) AND
        REGEXP(query.web_property, r"^ca-mb-app-|^ca-app-pub-|^ca-ha-app-pub-")
        = True) THEN 'app'
    WHEN (query.mobile_browser_class IN (0,1,2,4) AND
        REGEXP(query.web_property, r"^ca-mb-app-|^ca-app-pub-|^ca-ha-app-pub-")
        = False) THEN 'non_app_mob'
    WHEN query.mobile_browser_class = 3 THEN 'desktop'
  END platform,
  ROUND(((query.query_id.time_usec -
    impressions.impression.content_impression.iba_calibration_data.last_membership_timestamp_usec)
    / 1000000 / 60),2) AS delta_min
FROM processed_ads.AdEvents.yesterday
WHERE query.country = 'US'
  # Leaving conversions only.
  AND impressions.clicks.conversions.conversion.conversion_id.process_id
    IS NOT NULL
  # Check whether the data source is remarketing.
  AND impressions.impression.content_impression.iba_calibration_data.user_list_source
    IN (1, 30, 105, 106, 107, 193, 235, 265, 307)
  # Filetring out outliers in RKTM time query served.
  AND STRFTIME_USEC(impressions.impression.content_impression.iba_calibration_data.last_membership_timestamp_usec,
    '%Y%m%d') >= '20161201';



###############
# PRevious versions of script.


  SET accounting_group analytics-internal-processing-dev;
  SET min_completion_ratio 1;
  SET io_timeout 2400;
  SET runtime_name dremel;
  SET materialize_overwrite true;
  SET materialize_owner_group analytics-internal-processing-dev;

  # Delta impressions.
  MATERIALIZE '/cns/ig-d/home/aredakov/gdn_recency/delta_times_queries/data' AS
    SELECT
    customer_id,
    NTH(51,QUANTILES(delta_min,101)) AS median_delta
  FROM
  (SELECT
    impression_set.customer_id AS customer_id,
    ROUND(((query_id.time_usec  -
      impression_set.content_impression.iba_calibration_data.last_membership_timestamp_usec)
      / 1000000 / 60),2) AS delta_min
  FROM ads.AdQueries.yesterday
  WHERE # limit the user_list_source to real remarketing.
    impression_set.content_impression.iba_calibration_data.user_list_source
      IN (1, 105, 106, 107, 235)
    AND country IN ('US')
    AND STRFTIME_USEC(
      impression_set.content_impression.iba_calibration_data.last_membership_timestamp_usec,
     '%Y%m%d') >= '20161201')
  GROUP@50 BY 1;

  # Delta conversions.
  MATERIALIZE '/cns/ig-d/home/aredakov/gdn_recency/delta_times_conversions/data@500' AS
  SELECT
    customer_id,
    NTH(51,QUANTILES(delta_min,101)) AS median_delta
  FROM
  (SELECT
    impressions.impression.customer_id AS customer_id,
    query.country AS country,
    ROUND(((query.query_id.time_usec -
      impressions.impression.content_impression.iba_calibration_data.last_membership_timestamp_usec)
       / 1000000 / 60),2) AS delta_min
  FROM processed_ads.AdEvents.yesterday
  WHERE impressions.clicks.conversions.conversion.conversion_id.process_id IS NOT NULL
    AND impressions.impression.content_impression.iba_calibration_data.user_list_source
      IN (1, 105, 106, 107, 235)
    AND query.country IN ('US')
    AND STRFTIME_USEC(impressions.impression.content_impression.iba_calibration_data.last_membership_timestamp_usec,
      '%Y%m%d') >= '20161201')
  GROUP@50 BY 1;

  # Impressions distribution.
  MATERIALIZE '/cns/ig-d/home/aredakov/gdn_recency/delta_queries_distribution/data' AS
  SELECT
    impression_set.customer_id AS customer_id,
    ROUND(((query_id.time_usec  -
      impression_set.content_impression.iba_calibration_data.last_membership_timestamp_usec)
      / 1000000 / 60),2) AS delta_min
  FROM ads.AdQueries.yesterday
  WHERE # limit the user_list_source to real remarketing.
    impression_set.content_impression.iba_calibration_data.user_list_source
      IN (1, 105, 106, 107, 235)
    AND country IN ('US')
    AND STRFTIME_USEC(
      impression_set.content_impression.iba_calibration_data.last_membership_timestamp_usec,
     '%Y%m%d') >= '20161201';

  # Conversions distribution.
  MATERIALIZE '/cns/ig-d/home/aredakov/gdn_recency/delta_conversions_distribution/data@500' AS
  SELECT
    CASE
    WHEN (query.mobile_browser_class IN (0,1,2,4) AND
        REGEXP(query.web_property, r"^ca-mb-app-|^ca-app-pub-|^ca-ha-app-pub-") = True)
      THEN 'app'
    WHEN (query.mobile_browser_class IN (0,1,2,4) AND
        REGEXP(query.web_property, r"^ca-mb-app-|^ca-app-pub-|^ca-ha-app-pub-") = False)
      THEN 'non_app_mob'
    WHEN query.mobile_browser_class = 3 THEN 'desktop'
    END AS platform,
    impressions.clicks.conversions.conversion.conversion_id.process_id AS conversion_id,
    ROUND(((query.query_id.time_usec -
      impressions.impression.content_impression.iba_calibration_data.last_membership_timestamp_usec)
       / 1000000 / 60),2) AS delta_min
  FROM processed_ads.AdEvents.yesterday
  WHERE impressions.clicks.conversions.conversion.conversion_id.process_id IS NOT NULL
    AND impressions.impression.content_impression.iba_calibration_data.user_list_source
      IN (1, 105, 106, 107, 235)
    AND query.country IN ('US')
    AND STRFTIME_USEC(impressions.impression.content_impression.iba_calibration_data.last_membership_timestamp_usec,
      '%Y%m%d') >= '20161201';

  MATERIALIZE '/cns/ig-d/home/aredakov/gdn_recency/cids_verticals/data' AS
  SELECT
    customer_id,
    advertiser.adv_vertical AS adv_vertical
  FROM x360_core.X360_DailyHistoricalStats_F.20170201,
    x360_core.X360_DailyHistoricalStats_F.20170203,
    x360_core.X360_DailyHistoricalStats_F.20170205,
    x360_core.X360_DailyHistoricalStats_F.20170208,
    x360_core.X360_DailyHistoricalStats_F.20170210,
    x360_core.X360_DailyHistoricalStats_F.20170212,
    x360_core.X360_DailyHistoricalStats_F.20170215,
    x360_core.X360_DailyHistoricalStats_F.20170218,
    x360_core.X360_DailyHistoricalStats_F.20170220,
    x360_core.X360_DailyHistoricalStats_F.20170223,
    x360_core.X360_DailyHistoricalStats_F.20170225,
    x360_core.X360_DailyHistoricalStats_F.20170228
  WHERE advertiser.adv_vertical IN ('Travel','Retail','Consumer Packaged Goods',
    'Food & Beverages', 'Automotive','Healthcare')
  GROUP@5000 BY 1,2;

  MATERIALIZE '/cns/ig-d/home/aredakov/gdn_recency/cids_conversions/data' AS
  SELECT
    customer_id,
    NTH(51,QUANTILES(delta_min,101)) AS median_delta
  FROM
  (SELECT
    impressions.impression.customer_id AS customer_id,
    impressions.clicks.conversions.conversion.conversion_id.process_id AS conversion_id,
    ROUND(((query.query_id.time_usec -
      impressions.impression.content_impression.iba_calibration_data.last_membership_timestamp_usec)
       / 1000000 / 60),2) AS delta_min
  FROM FLATTEN(processed_ads.AdEvents.yesterday,impressions)
  WHERE impressions.clicks.conversions.conversion.conversion_id.process_id IS NOT NULL
    AND impressions.impression.content_impression.iba_calibration_data.user_list_source
      IN (1, 105, 106, 107, 235)
    AND query.country IN ('US')
    AND query.mobile_browser_class = 3 #'desktop'
    AND STRFTIME_USEC(impressions.impression.content_impression.iba_calibration_data.last_membership_timestamp_usec,
      '%Y%m%d') >= '20161201')
  GROUP@50 BY 1;

  DEFINE TABLE verticals_cids /cns/ig-d/home/aredakov/gdn_recency/cids_verticals/data*;
  DEFINE TABLE cids_conversions /cns/ig-d/home/aredakov/gdn_recency/cids_conversions/data*;
  MATERIALIZE '/cns/ig-d/home/aredakov/gdn_recency/verticals_conversions/data' AS
  SELECT
    adv_vertical,
    median_delta
  FROM cids_conversions a
  JOIN@500 verticals_cids b
  ON a.customer_id = b.customer_id
  WHERE median_delta < 50000;

  library(ginstall)
  library(gfile)
  library(namespacefs)
  library(rglib)
  library(cfs)
  library(dremel)
  library(Hmisc)
  library(ggplot2)
  library(scales)
  library(directlabels)
  library(lubridate)
  library(boot)
  library(gmp)
  library(MASS)
  InitGoogle()
  options("scipen"=100, "digits"=12)

  myConn <- DremelConnect()
  DremelSetMinCompletionRatio(myConn, 1.0)
  DremelSetAccountingGroup(myConn,'urchin-processing-qa')
  DremelSetMaterializeOwnerGroup(myConn, 'materialize-a-dremel')
  DremelSetMaterializeOverwrite(myConn, TRUE)
  DremelSetIOTimeout(myConn, 7200)

  #################
  # Verticals.
  DremelAddTableDef('verticals_conversions', '/cns/ig-d/home/aredakov/gdn_recency/verticals_conversions/data*',
    myConn, verbose=FALSE)

  v <- DremelExecuteQuery("
    SELECT *
    FROM verticals_conversions
  ;", myConn)

  ggplot(v, aes(x = median_delta, fill = adv_vertical)) +
  geom_density(alpha = 0.5) +
  facet_grid(adv_vertical ~.) +
  theme(strip.text=element_text(size=11,face="bold")) +
  theme(axis.text.x=element_text(size=11,face="bold", color="gray26")) +
  theme(axis.text.y=element_text(size=11,face="bold", color="gray26")) +
  theme(legend.position="none") +
  theme(legend.title = element_blank()) +
  ggtitle("") +
  xlab("Min")

  ggplot(v ,aes(x = delta_min, y = cnt_conversions)) +
  geom_bar(stat = "identity") +
  theme_bw() +
  theme(legend.position="none") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

  #################
  # Impressions Distribution.
  DremelAddTableDef('delta_queries_distribution', '/cns/ig-d/home/aredakov/gdn_recency/delta_queries_distribution/data*',
    myConn, verbose=FALSE)

  d <- DremelExecuteQuery("
    SELECT delta_min, COUNT(*) AS queries
    FROM delta_queries_distribution
    WHERE delta_min <= 50000
    GROUP@50 BY 1
  ;", myConn)

  dt <- DremelExecuteQuery("
    SELECT COUNT(*) AS total_queries
    FROM delta_queries_distribution
    WHERE delta_min <= 50000
  ;", myConn)

  qs <- d[ which(d$delta_min <= 600), ]
  qs$bin <- cut2(qs$delta_min, c(120,180,240,300,360,420,480,540,600))
  sums <- ddply(qs, .(bin), summarise, total = sum(queries))
  sums$que_share <- sums$total/dt$total_queries

  ggplot(sums, aes(x=bin, y=que_share, fill="red")) +
  geom_bar(stat="identity") +
  scale_y_continuous(labels = percent) +
  theme_bw() +
  theme(legend.position="none") +
  geom_text(aes(label=sprintf("%1.1f%%", que_share*100)),vjust=+1.1,
    size=5) +
  theme(strip.text=element_text(size=11,face="bold")) +
  theme(axis.text.x=element_text(size=11,face="bold", color="gray26")) +
  theme(axis.text.y=element_text(size=11,face="bold", color="gray26")) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  theme(axis.ticks.y = element_blank()) +
  theme(axis.text.y = element_blank()) +
  theme(axis.line.y = element_blank()) +
  ggtitle("Share of Impressions over Recency in Minutes.") +
  ylab("Share of Queries") + xlab("Bins in Minutes")

  # Conversions Distribution.
  DremelAddTableDef('delta_conversions_distribution', '/cns/ig-d/home/aredakov/gdn_recency/delta_conversions_distribution/data*',
    myConn, verbose=FALSE)

  co <- DremelExecuteQuery("
    SELECT
      platform,
      delta_min,
      COUNT(*) AS conversions
    FROM delta_conversions_distribution
    WHERE delta_min <= 50000
      AND platform !=''
    GROUP@50 BY 1,2
  ;", myConn)

  # Density plot.
  ggplot(co, aes(x = delta_min, fill = platform)) +
  geom_density(alpha = 0.5) +
  facet_grid(platform ~.) +
  theme(strip.text=element_text(size=11,face="bold")) +
  theme(axis.text.x=element_text(size=11,face="bold", color="gray26")) +
  theme(axis.text.y=element_text(size=11,face="bold", color="gray26")) +
  theme(legend.position="none") +
  theme(legend.title = element_blank()) +
  ggtitle("") +
  xlab("Min")

  to <- DremelExecuteQuery("
    SELECT
      platform,
      COUNT(delta_min) AS total_conversions
    FROM delta_conversions_distribution
    WHERE delta_min <= 50000
      AND platform !=''
    GROUP@50 BY 1
  ;", myConn)

  # All platforms.
  #   co$bin <- cut2(co$delta_min, c(120,180,240,300,360,420,480,540,600))
  #   co <- ddply(co, .(bin), summarise, total = sum(conversions))
  #   co$share <- co$total/to$total_conversions

  # Apps.
  a <- co[ which(co$platform=='app' & co$delta_min <= 600), ]
  b <- to[ which(to$platform=='app'), ]
  a$bin <- cut2(a$delta_min, c(120,180,240,300,360,420,480,540,600))
  a <- ddply(a, .(bin), summarise, total = sum(conversions))
  a$share <- a$total/b$total_conversions

  # Desktop.
  c <- co[ which(co$platform=='desktop' & co$delta_min <= 600), ]
  d <- to[ which(to$platform=='desktop'), ]
  c$bin <- cut2(c$delta_min, c(120,180,240,300,360,420,480,540,600))
  c <- ddply(c, .(bin), summarise, total = sum(conversions))
  c$share <- c$total/d$total_conversions

  # non_app_mob.
  e <- co[ which(co$platform=='non_app_mob' & co$delta_min <= 600), ]
  f <- to[ which(to$platform=='non_app_mob'), ]
  e$bin <- cut2(e$delta_min, c(120,180,240,300,360,420,480,540,600))
  e <- ddply(e, .(bin), summarise, total = sum(conversions))
  e$share <- e$total/f$total_conversions

  ggplot(a, aes(x=bin,y=share)) +
  geom_bar(fill="red", stat="identity", alpha=0.6) +
  scale_y_continuous(labels = percent) +
  coord_cartesian(ylim = c(0, 0.2)) +
  theme_bw() +
  theme(legend.position="none") +
  geom_text(aes(label=sprintf("%1.1f%%", share*100)),vjust=-0.2,
    size=5) +
  theme(strip.text=element_text(size=11,face="bold")) +
  theme(axis.text.x=element_text(size=11,face="bold", color="gray26")) +
  theme(axis.text.y=element_text(size=11,face="bold", color="gray26")) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  theme(axis.ticks.y = element_blank()) +
  theme(axis.text.y = element_blank()) +
  theme(axis.line.y = element_blank()) +
  ggtitle("Share of Conversions over Recency in Minutes.") +
  ylab("Share of Conversions") + xlab("Bins in Minutes")

  # Cummulative.
  co$bin <- cut2(co$delta_min, g=12)
  sums <- ddply(co, .(bin), summarise, total = sum(conversions))
  sums$con_share <- sums$total/to$total_conversions

  pd <- position_dodge(.1)
  ggplot(sums, aes(x=bin, y=cumsum(con_share),fill="red")) +
  geom_bar(stat="identity") +
  geom_text(aes(label=sprintf("%1.1f%%", cumsum(con_share)*100)),vjust=+1.1,
  size=5) +
  scale_y_continuous(labels = percent) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  theme_bw() +
  theme(legend.position="none") +
  theme(strip.text=element_text(size=11,face="bold")) +
  theme(axis.text.x=element_text(size=11,face="bold", color="gray26")) +
  theme(axis.text.y=element_text(size=11,face="bold", color="gray26")) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  theme(axis.ticks.y = element_blank()) +
  theme(axis.text.y = element_blank()) +
  theme(axis.line.y = element_blank()) +
  ggtitle("Cumulative Share of Conversions over Recency in Minutes.") +
  ylab("Cumulative Share of Conversions") + xlab("Bins in Minutes")

  ##############
  # Density plots.
  # Conversions median.
  DremelAddTableDef('delta_times_conversions', '/cns/ig-d/home/aredakov/gdn_recency/delta_times_conversions/data*',
    myConn, verbose=FALSE)

  g <- DremelExecuteQuery("
    SELECT median_delta
    FROM delta_times_conversions
  ;", myConn)

  ggplot(g, aes(x = median_delta, fill = 'red')) +
  geom_density(alpha = 0.5) +
  theme(legend.position="none") +
  theme(legend.title = element_blank()) +
  ggtitle("Conversions. Recency in Min: Median Delta.
    Single Day Data") +
  xlab("Min")

  # Impressions median.
  DremelAddTableDef('delta_times_queries', '/cns/ig-d/home/aredakov/gdn_recency/delta_times_queries/data*',
    myConn, verbose=FALSE)

  i <- DremelExecuteQuery("
    SELECT median_delta
    FROM delta_times_queries
  ;", myConn)

  ggplot(i, aes(x = median_delta, fill = 'red')) +
  geom_density(alpha = 0.5) +
  theme(legend.position="none") +
  theme(strip.text=element_text(size=11,face="bold")) +
  theme(axis.text.x=element_text(size=11,face="bold", color="gray26")) +
  theme(axis.text.y=element_text(size=11,face="bold", color="gray26")) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  theme(legend.title = element_blank()) +
  ggtitle("Impressions. Recency in Min: Median Delta.
    Single Day Data") +
  xlab("Min")
